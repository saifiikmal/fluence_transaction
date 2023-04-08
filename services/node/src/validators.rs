use crate::block::Block;
use crate::cron::{Cron, SerdeCron};
use crate::data_types::DataTypeClone;
use crate::defaults::CRON_ACTION_CREATE;
use crate::metadatas::{FinalMetadata, Metadata};
use crate::transaction::TransactionSubset;
use crate::{defaults::STATUS_FAILED, defaults::STATUS_SUCCESS};
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

    let storage = get_storage().expect("Internal error to database connector");

    let mut transaction = storage.get_transaction(transaction_hash).unwrap().clone();

    let sm_result = storage.get_meta_contract(transaction.token_key.clone());

    match sm_result {
        Ok(contract) => {
            if transaction.public_key != contract.public_key {
                error = Some(InvalidOwner(f!("{transaction.public_key}")))
            } else {
                current_meta_contract = contract;
                current_meta_contract.meta_contract_id = transaction.data.clone();
            }
            is_update = true;
        }
        Err(ServiceError::RecordNotFound(_)) => {}
        Err(e) => error = Some(e),
    }

    if error.is_none() {
        let meta_result;

        if !is_update {
            current_meta_contract = MetaContract {
                token_key: transaction.token_key.clone(),
                meta_contract_id: transaction.meta_contract_id.clone(),
                public_key: transaction.public_key.clone(),
            };

            meta_result = storage.write_meta_contract(current_meta_contract);
        } else {
            meta_result = storage.rebind_meta_contract(
                transaction.token_key.clone(),
                transaction.meta_contract_id.clone(),
            );
        }

        match meta_result {
            Ok(()) => {}
            Err(e) => error = Some(e),
        }
    }

    if !error.is_none() {
        transaction.error_text = error.unwrap().to_string();
        transaction.status = STATUS_FAILED;
    } else {
        transaction.status = STATUS_SUCCESS;
        transaction.error_text = "".to_string();
    }

    let _ = storage.update_transaction_status(
        transaction.hash.clone(),
        transaction.status.clone(),
        transaction.error_text.clone(),
    );
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
    let storage = get_storage().expect("Internal error to database connector");
    let mut transaction = storage.get_transaction(transaction_hash).unwrap().clone();

    if !on_metacontract_result {
        transaction.status = STATUS_FAILED;
        if final_error_msg.is_empty() {
            transaction.error_text = "Metadata not updateable".to_string();
        } else {
            transaction.error_text = final_error_msg;
        }
    } else {
        for data in metadatas {
            let result = storage.get_owner_metadata_by_datakey_and_alias(
                transaction.data_key.clone(),
                data.public_key.clone(),
                data.alias.clone(),
            );

            log::info!("{:?}", result);

            match result {
                Ok(metadata) => {
                    transaction.status = STATUS_SUCCESS;

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

                    let _ = storage.update_cid(metadata.data_key, metadata.public_key, content_cid);
                }
                Err(ServiceError::RecordNotFound(_)) => {
                    transaction.status = STATUS_SUCCESS;

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

                    let metadata = Metadata::new(
                        transaction.data_key.clone(),
                        data.alias.clone(),
                        content_cid,
                        data.public_key.clone(),
                    );

                    let _ = storage.write_metadata(metadata);
                }
                Err(e) => {
                    transaction.error_text = e.to_string();
                    transaction.status = STATUS_FAILED;
                }
            };
        }
    }

    let _ = storage.update_transaction_status(
        transaction.hash.clone(),
        transaction.status.clone(),
        transaction.error_text.clone(),
    );
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
    let storage = get_storage().expect("Internal error to database connector");
    let mut transaction = storage.get_transaction(transaction_hash).unwrap().clone();

    if !on_metacontract_result {
        transaction.status = STATUS_FAILED;
        if final_error_msg.is_empty() {
            transaction.error_text = "Metadata not forkable".to_string();
        } else {
            transaction.error_text = final_error_msg;
        }
    } else {
        let data_clone: DataTypeClone = serde_json::from_str(&data.clone()).unwrap();

        let origin_metadata = storage
            .get_owner_metadata_by_datakey_and_alias(
                data_clone.origin_data_key.clone(),
                data_clone.origin_public_key.clone(),
                data_clone.origin_alias.clone(),
            )
            .unwrap();

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
            transaction.alias.clone(),
            result_ipfs_dag_put.cid,
            transaction.public_key.clone(),
        );

        let _ = storage.write_metadata(metadata);

        transaction.status = STATUS_SUCCESS;
    }

    let _ = storage.update_transaction_status(
        transaction.hash.clone(),
        transaction.status.clone(),
        transaction.error_text.clone(),
    );
}

/**
 * Validated "cron" method type
 */
pub fn validate_cron(transaction_hash: String, data: String) {
    let storage = get_storage().expect("Internal error to database connector");
    let mut transaction = storage.get_transaction(transaction_hash).unwrap().clone();

    let serde_cron: SerdeCron = serde_json::from_str(&data).unwrap();

    if serde_cron.action == CRON_ACTION_CREATE {
        let cron = Cron::new(
            serde_cron.address,
            serde_cron.topic,
            serde_cron.token_type,
            serde_cron.chain,
        );

        let _ = storage.write_cron(cron);
    } else {
        let _ = storage.update_cron_status(serde_cron.cron_id, serde_cron.status);
    }

    transaction.status = STATUS_SUCCESS;
    transaction.error_text = "".to_string();

    let _ = storage.update_transaction_status(
        transaction.hash.clone(),
        transaction.status.clone(),
        transaction.error_text.clone(),
    );
}
