use std::time::{UNIX_EPOCH, SystemTime};

use crate::block::Block;
use crate::cron::{Cron, SerdeCron};
use crate::data_types::DataTypeClone;
use crate::defaults::{CRON_ACTION_CREATE, CRON_ACTION_UPDATE, CRON_ACTION_UPDATE_STATUS, CRON_STATUS_ENABLE, RECEIPT_STATUS_FAILED, RECEIPT_STATUS_SUCCESS, STATUS_DONE, STATUS_FAILED};
use crate::metadatas::{FinalMetadata, Metadata, SerdeMetadata};
use crate::result::{FdbMetadatasResult};
use crate::transaction::{TransactionSubset, TransactionReceipt};
use crate::{error::ServiceError, error::ServiceError::*};
use crate::{get, put_block};
use crate::{meta_contract::MetaContract, storage_impl::get_storage};

/**
 * Validated meta contract method type
 */
pub fn validate_meta_contract(transaction_hash: String) {
    let mut current_meta_contract;
    let mut is_update = false;
    let mut error: Option<ServiceError> = None;

    let storage = get_storage();

    let mut transaction = storage.get_transaction(transaction_hash).unwrap().clone();

    let sm_result = storage.get_meta_contract_by_id_and_pk(transaction.meta_contract_id.clone(), transaction.public_key.clone());

    let mut is_update = false;
    match sm_result {
        Ok(contract) => {
            if transaction.public_key == contract.public_key {
                // error = Some(RecordFound(f!("{transaction.public_key}")))
                is_update = true;
            } else {
                current_meta_contract = contract;
                current_meta_contract.meta_contract_id = transaction.data.clone();
            }
        }
        Err(ServiceError::RecordNotFound(_)) => {}
        Err(e) => error = Some(e),
    }

    if error.is_none() {
        let meta_result;

        current_meta_contract = MetaContract::new(
            transaction.token_key.clone(),
            transaction.meta_contract_id.clone(),
            transaction.public_key.clone(),
            "".to_string(),
        );

        if is_update {
          meta_result = storage.rebind_meta_contract(
            transaction.token_key.clone(), 
            transaction.meta_contract_id.clone(), 
            transaction.public_key.clone(),
          );
        } else {
          meta_result = storage.write_meta_contract(current_meta_contract);
        }

        match meta_result {
            Ok(()) => {}
            Err(e) => error = Some(e),
        }
    }

    let mut status;
    let mut error_text;
    let now = SystemTime::now();
    let timestamp = now.duration_since(UNIX_EPOCH).expect("Time went backwards");

    if !error.is_none() {
        error_text = error.unwrap().to_string();
        status = RECEIPT_STATUS_FAILED;
    } else {
        status = RECEIPT_STATUS_SUCCESS;
        error_text = "".to_string();
    }

    let receipt = TransactionReceipt {
      hash: transaction.hash.clone(),
      meta_contract_id: transaction.meta_contract_id.clone(),
      status,
      timestamp: timestamp.as_millis() as u64,
      error_text,
      data: "".to_string(),
    };
    storage.write_transaction_receipt(receipt);

    storage.update_transaction_status(transaction.hash.clone(), STATUS_DONE);
}

/**
 * Validated "metadata" method type
 */
pub fn validate_metadata(
    transaction_hash: String,
    meta_contract_id: String,
    on_metacontract_result: bool,
    metadatas: Vec<FinalMetadata>,
    final_error_msg: String,
) {
    let storage = get_storage();
    let mut transaction = storage.get_transaction(transaction_hash).unwrap().clone();

    let mut status = RECEIPT_STATUS_SUCCESS;
    let mut error_text = "".to_string();

    if !on_metacontract_result {
        if final_error_msg.is_empty() {
            error_text = "Metadata not updateable".to_string();
        } else {
            error_text = final_error_msg;
        }
        status = RECEIPT_STATUS_FAILED;
    } else {
        for data in metadatas {
            let result = storage.get_owner_metadata(
                transaction.data_key.clone(),
                transaction.meta_contract_id.clone(),
                data.public_key.clone(),
                data.alias.clone(),
                transaction.version.clone(),
            );

            log::info!("{:?}", result);

            match result {
                Ok(metadata) => {

                    let tx = TransactionSubset {
                        hash: transaction.hash.clone(),
                        timestamp: transaction.timestamp.clone(),
                        meta_contract_id: meta_contract_id.clone(),
                        method: transaction.method.clone(),
                        value: "".to_string(),
                    };

                    let tx_serde = serde_json::to_string(&tx).unwrap();

                    let result_ipfs_dag_put =
                        put_block(data.content, metadata.cid, tx_serde, "".to_string(), 0);
                    let content_cid = result_ipfs_dag_put.cid;

                    let _ = storage.update_cid(
                        metadata.data_key, 
                        metadata.meta_contract_id, 
                        metadata.alias, 
                        metadata.public_key, 
                        content_cid,
                        metadata.version,
                    );
                    
                    status = RECEIPT_STATUS_SUCCESS;
                }
                Err(ServiceError::RecordNotFound(_)) => {

                    let tx = TransactionSubset {
                        hash: transaction.hash.clone(),
                        timestamp: transaction.timestamp.clone(),
                        meta_contract_id: meta_contract_id.clone(),
                        method: transaction.method.clone(),
                        value: "".to_string(),
                    };

                    let tx_serde = serde_json::to_string(&tx).unwrap();

                    let result_ipfs_dag_put =
                        put_block(data.content, "".to_string(), tx_serde, "".to_string(), 0);
                    let content_cid = result_ipfs_dag_put.cid;

                    let serde_metadata: Result<SerdeMetadata, serde_json::Error> = serde_json::from_str(&transaction.mcdata.clone());

                    let mut loose;
                    match serde_metadata {
                      Ok(sm) => loose = sm.loose,
                      _ => loose = 1,
                    }

                    let metadata = Metadata::new(
                        transaction.data_key.clone(),
                        transaction.token_key.clone(),
                        transaction.meta_contract_id.clone(),
                        transaction.token_id.clone(),
                        data.alias.clone(),
                        content_cid,
                        data.public_key.clone(),
                        transaction.version.clone(),
                        loose,
                    );

                    let _ = storage.write_metadata(metadata);

                    status = RECEIPT_STATUS_SUCCESS;
                }
                Err(e) => {
                    error_text = e.to_string();
                    status = RECEIPT_STATUS_FAILED;
                }
            };
        }
    }

    let now = SystemTime::now();
    let timestamp = now.duration_since(UNIX_EPOCH).expect("Time went backwards");

    let receipt = TransactionReceipt {
      hash: transaction.hash.clone(),
      meta_contract_id: transaction.meta_contract_id.clone(),
      status,
      timestamp: timestamp.as_millis() as u64,
      error_text,
      data: "".to_string(),
    };
    storage.write_transaction_receipt(receipt);

    storage.update_transaction_status(transaction.hash.clone(), STATUS_DONE);
}

/**
 * Validated "metadata cron" method type
 */
pub fn validate_metadata_cron(
  meta_contract: MetaContract,
  cron: Cron,
  token_id: String,
  on_metacontract_result: bool,
  metadatas: Vec<FinalMetadata>,
) -> FdbMetadatasResult {
  let mut final_metadatas: Vec<Metadata> = Vec::new();
  let mut err_msg = "".to_string();
  let storage = get_storage();

  if on_metacontract_result {
      let data_key = Metadata::generate_data_key(
        cron.chain, 
        cron.address, 
        token_id.clone(),
      );
      for data in metadatas {
          let result = storage.get_owner_metadata(
              data_key.clone(),
              meta_contract.meta_contract_id.clone(),
              data.public_key.clone(),
              data.alias.clone(),
              data.version.clone(),
          );

          log::info!("{:?}", result);

          match result {
              Ok(data) => {
                final_metadatas.push(data);
              }
              Err(ServiceError::RecordNotFound(_)) => {

                  let result_ipfs_dag_put =
                      put_block(data.content, "".to_string(), "{}".to_string(), "".to_string(), 0);
                  let content_cid = result_ipfs_dag_put.cid;

                  let metadata = Metadata::new(
                      data_key.clone(),
                      cron.token_key.clone(),
                      meta_contract.meta_contract_id.clone(),
                      token_id.clone(),
                      data.alias.clone(),
                      content_cid,
                      data.public_key.clone(),
                      data.version.clone(),
                      data.loose.clone(),
                  );

                  let _ = storage.write_metadata(metadata.clone());
                  final_metadatas.push(metadata);
              }
              Err(e) => {
                err_msg = e.to_string();
              }
          };
      }
  }
  FdbMetadatasResult { 
    success: on_metacontract_result, 
    err_msg, 
    metadatas: final_metadatas, 
  }
}

/**
 * Validated "clone" method type
 * Fetch the origin metadata content from Block and clone it to the new metadata
 */
pub fn validate_clone(
    transaction_hash: String,
    meta_contract_id: String,
    on_metacontract_result: bool,
    data: String,
    final_error_msg: String,
) {
    let storage = get_storage();
    let mut transaction = storage.get_transaction(transaction_hash).unwrap().clone();

    let mut status = RECEIPT_STATUS_FAILED;
    let mut error_text = "".to_string();
    if !on_metacontract_result {
        status = RECEIPT_STATUS_FAILED;
        if final_error_msg.is_empty() {
            error_text = "Metadata not forkable".to_string();
        } else {
            error_text = final_error_msg;
        }
    } else {
        let data_clone: DataTypeClone = serde_json::from_str(&data.clone()).unwrap();

        let origin_metadata = storage.get_owner_metadata(
            data_clone.origin_data_key.clone(),
            data_clone.origin_meta_contract_id.clone(),
            data_clone.origin_public_key.clone(),
            data_clone.origin_alias.clone(),
            data_clone.origin_version.clone(),
        ).unwrap();

        let tx = TransactionSubset {
            hash: transaction.hash.clone(),
            timestamp: transaction.timestamp.clone(),
            meta_contract_id: meta_contract_id.clone(),
            method: transaction.method.clone(),
            value: serde_json::to_string(&data_clone).unwrap(),
        };

        let tx_serde = serde_json::to_string(&tx).unwrap();

        let ipfs_get_result = get(origin_metadata.cid, "".to_string(), 0);

        let block: Block = serde_json::from_str(&ipfs_get_result.block).unwrap();
        let content = block.content;

        let result_ipfs_dag_put = put_block(
            serde_json::to_string(&content).unwrap(),
            "".to_string(),
            tx_serde,
            "".to_string(),
            0,
        );

        let metadata = Metadata::new(
            transaction.data_key.clone(),
            origin_metadata.token_key.clone(),
            origin_metadata.meta_contract_id.clone(),
            origin_metadata.token_id.clone(),
            origin_metadata.alias.clone(),
            result_ipfs_dag_put.cid,
            origin_metadata.public_key.clone(),
            origin_metadata.version.clone(),
            origin_metadata.loose.clone(),
        );

        let _ = storage.write_metadata(metadata);

        status = RECEIPT_STATUS_SUCCESS;
    }

    let now = SystemTime::now();
    let timestamp = now.duration_since(UNIX_EPOCH).expect("Time went backwards");

    let receipt = TransactionReceipt {
      hash: transaction.hash.clone(),
      meta_contract_id: transaction.meta_contract_id.clone(),
      status,
      timestamp: timestamp.as_millis() as u64,
      error_text,
      data: "".to_string(),
    };
    storage.write_transaction_receipt(receipt);

    storage.update_transaction_status(transaction.hash.clone(), STATUS_DONE);
}

/**
 * Validated "cron" method type
 */
pub fn validate_cron(transaction_hash: String) {
    let mut status = RECEIPT_STATUS_SUCCESS;
    let mut error_text = "".to_string();

    let storage = get_storage();
    let mut transaction = storage.get_transaction(transaction_hash).unwrap().clone();

    let serde_cron: SerdeCron = serde_json::from_str(&transaction.data).unwrap();

    let result = storage.search_cron(serde_cron.address.clone(), 
      serde_cron.chain.clone(), serde_cron.topic.clone());

      let mut cron = Cron::new(
        transaction.token_key.clone(),
        serde_cron.address,
        serde_cron.topic,
        serde_cron.token_type,
        serde_cron.chain,
        serde_cron.status,
        transaction.meta_contract_id.clone(),
        serde_cron.node_url,
        transaction.public_key.clone(),
        serde_cron.abi_url,
        0,
    );

    match result {
      Ok(row) => {
        if transaction.public_key == row.public_key {
          if !serde_cron.hash.is_empty() {
            match serde_cron.action.as_str() {
              CRON_ACTION_UPDATE => {
                let _ = storage.update_cron(serde_cron.hash, cron);
              }
              CRON_ACTION_UPDATE_STATUS => {
                let _ = storage.update_cron_status(serde_cron.hash, serde_cron.status);
              }
              _ => {
                status = STATUS_FAILED;
                error_text = "Invalid cron action".to_string();
              }
            }
          } else {
            status = STATUS_FAILED;
            error_text = format!("Invalid cron hash: {}", serde_cron.hash);
          }
        } else {
          status = STATUS_FAILED;
          error_text = format!("Invalid owner: {}", transaction.public_key);
        }
      }
      Err(ServiceError::RecordNotFound(_)) => {
        if serde_cron.action == CRON_ACTION_CREATE {
          cron.status = CRON_STATUS_ENABLE;
  
          let _ = storage.write_cron(cron);
        } else {
          status = STATUS_FAILED;
          error_text = "Invalid cron action".to_string();
        }
      }
      Err(_) => {
        status = STATUS_FAILED;
        error_text = "Invalid cron".to_string();
      }
    }

    let now = SystemTime::now();
    let timestamp = now.duration_since(UNIX_EPOCH).expect("Time went backwards");

    let receipt = TransactionReceipt {
      hash: transaction.hash.clone(),
      meta_contract_id: transaction.meta_contract_id.clone(),
      status,
      timestamp: timestamp.as_millis() as u64,
      error_text,
      data: "".to_string(),
    };
    storage.write_transaction_receipt(receipt);

    storage.update_transaction_status(transaction.hash.clone(), STATUS_DONE);
}
