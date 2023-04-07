#![allow(improper_ctypes)]

pub mod cron;
mod defaults;
mod error;
mod meta_contract;
mod meta_contract_impl;
mod metadatas;
mod metadatas_impl;
mod result;
mod storage_impl;
mod transaction;
pub mod transactions_impl;
mod validators;

use cron::SerdeCron;
use defaults::{
    CRON_ACTION_CREATE, CRON_STATUS_ACTIVE, CRON_STATUS_DISABLE, ENCRYPTION_TYPE_ED25519,
    ENCRYPTION_TYPE_SECP256K1, METHOD_CRON,
};
use defaults::{METHOD_CLONE, METHOD_CONTRACT, METHOD_METADATA, STATUS_FAILED};
use marine_rs_sdk::marine;
use marine_rs_sdk::module_manifest;
use marine_rs_sdk::WasmLoggerBuilder;

use error::ServiceError::{
    self, InvalidMethod, InvalidOwner, InvalidSignature, NoEncryptionType,
    NotSupportedEncryptionType, RecordFound,
};

use metadatas::FinalMetadata;
use result::{
    FdbCronsResult, FdbMetaContractResult, FdbMetadataHistoryResult, FdbMetadatasResult,
    FdbTransactionResult, FdbTransactionsResult,
};
use result::{FdbMetadataResult, FdbResult};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use storage_impl::get_storage;
use transaction::Transaction;
use types::{IpfsDagGetResult, IpfsDagPutResult};
use validators::{validate_clone, validate_meta_contract, validate_metadata};

#[macro_use]
extern crate fstrings;

module_manifest!();

pub fn wrapped_try<F, T>(func: F) -> T
where
    F: FnOnce() -> T,
{
    func()
}

pub fn main() {
    WasmLoggerBuilder::new()
        .with_log_level(log::LevelFilter::Info)
        .build()
        .unwrap();

    let storage = get_storage().unwrap();
    storage.create_meta_contract_table();
    storage.create_transactions_table();
    storage.create_metadatas_table();
    storage.create_cron_table()
}

#[marine]
pub fn send_transaction(
    data_key: String,
    token_key: String,
    token_id: String,
    alias: String,
    public_key: String,
    signature: String,
    data: String,
    method: String,
    nonce: i64,
) -> FdbResult {
    let mut meta_contract_id = "".to_string();
    let mut error: Option<ServiceError> = None;
    let storage = get_storage().expect("Database non existance");

    if error.is_none() {
        if method != METHOD_CONTRACT && method != METHOD_METADATA && method != METHOD_CLONE {
            error = Some(InvalidMethod(f!("invalid method: {method}")));
        }
    }

    let enc_verify = get_public_key_type(public_key.clone().as_str());
    if enc_verify.len() <= 0 {
        error = Some(ServiceError::InvalidEncryption(public_key.clone()));
    }

    if error.is_none() {
        if method.clone() == METHOD_METADATA {
            let result = storage.get_owner_metadata_by_datakey_and_alias(
                data_key.clone(),
                public_key.clone(),
                alias.clone(),
            );

            match result {
                Ok(metadata) => {
                    if metadata.public_key != public_key {
                        error = Some(InvalidOwner(f!("not owner of data_key: {public_key}")));
                    }
                }
                Err(ServiceError::RecordNotFound(_)) => {}
                Err(e) => error = Some(e),
            }
        } else if method.clone() == METHOD_CONTRACT {
            meta_contract_id = data.clone();
        } else if method.clone() == METHOD_CLONE {
            let old_metadata_result = storage.get_owner_metadata_by_datakey_and_alias(
                data.clone(),
                public_key.clone(),
                alias.clone(),
            );

            match old_metadata_result {
                Ok(_) => {}
                Err(e) => error = Some(e),
            }

            let new_metadata_result = storage.get_owner_metadata_by_datakey_and_alias(
                data_key.clone(),
                public_key.clone(),
                alias.clone(),
            );

            match new_metadata_result {
                Ok(_) => error = Some(RecordFound(data_key.clone())),
                Err(ServiceError::RecordNotFound(_)) => {}
                Err(e) => error = Some(e),
            }
        } else if method.clone() == METHOD_CRON {
            let cron_result: Result<SerdeCron, serde_json::Error> = serde_json::from_str(&data);

            match cron_result {
                Ok(serde_cron) => {
                    if serde_cron.action == CRON_ACTION_CREATE {
                        if serde_cron.address.len() <= 0
                            || serde_cron.chain.len() <= 0
                            || serde_cron.topic.len() <= 0
                            || serde_cron.token_type.len() <= 0
                        {
                            error =
                                Some(ServiceError::InvalidDataFormatForMethodType(method.clone()))
                        } else {
                            let result = storage.search_cron(
                                serde_cron.address.clone(),
                                serde_cron.chain.clone(),
                                serde_cron.topic.clone(),
                            );

                            match result {
                                Ok(_) => {
                                    error = Some(RecordFound(f!(
                                    "{serde_cron.address} {serde_cron.chain} {serde_cron.topic}"
                                )))
                                }
                                Err(ServiceError::RecordNotFound(_)) => {}
                                Err(e) => error = Some(e),
                            }
                        }
                    } else {
                        if serde_cron.cron_id <= 0
                            || (serde_cron.status != CRON_STATUS_ACTIVE
                                || serde_cron.status != CRON_STATUS_DISABLE)
                        {
                            error =
                                Some(ServiceError::InvalidDataFormatForMethodType(method.clone()))
                        } else {
                            let result = storage.get_cron_by_id(serde_cron.cron_id);
                            match result {
                                Ok(_) => {}
                                Err(e) => error = Some(e),
                            }
                        }
                    }
                }
                Err(e) => error = Some(ServiceError::InvalidDataFormatForMethodType(e.to_string())),
            }
        }
    }

    if error.is_none() {
        if enc_verify.clone().is_empty() {
            error = Some(NoEncryptionType())
        } else {
            if enc_verify.clone().ne(ENCRYPTION_TYPE_SECP256K1)
                && enc_verify.clone().ne(ENCRYPTION_TYPE_ED25519)
            {
                error = Some(NotSupportedEncryptionType(enc_verify.clone()));
            }
        }
    }

    if error.is_none() {
        let v = verify(
            public_key.clone(),
            signature.clone(),
            data.clone(),
            enc_verify.clone(),
        );

        if !v {
            error = Some(InvalidSignature(f!("not owner of data_key: {public_key}")));
        }
    }

    let cp = marine_rs_sdk::get_call_parameters();

    let now = SystemTime::now();
    let timestamp = now.duration_since(UNIX_EPOCH).expect("Time went backwards");

    let mut transaction = Transaction::new(
        token_key,
        cp.init_peer_id,
        cp.host_id,
        data_key,
        nonce,
        data,
        public_key,
        alias,
        timestamp.as_millis() as u64,
        meta_contract_id,
        method,
        token_id,
    );

    if !error.is_none() {
        transaction.error_text = error.unwrap().to_string();
        transaction.status = STATUS_FAILED;
    }

    let _ = storage.write_transaction(transaction.clone());

    FdbResult {
        transaction_hash: transaction.hash,
    }
}

#[marine]
pub fn get_transaction(hash: String) -> FdbTransactionResult {
    wrapped_try(|| get_storage()?.get_transaction(hash)).into()
}

#[marine]
pub fn get_metadata(data_key: String, public_key: String, alias: String) -> FdbMetadataResult {
    wrapped_try(|| {
        get_storage()?.get_owner_metadata_by_datakey_and_alias(data_key, public_key, alias)
    })
    .into()
}

#[marine]
pub fn get_metadatas(data_key: String) -> FdbMetadatasResult {
    wrapped_try(|| get_storage()?.get_metadata_by_datakey(data_key)).into()
}

#[marine]
pub fn get_meta_contract(token_key: String) -> FdbMetaContractResult {
    wrapped_try(|| get_storage()?.get_meta_contract(token_key)).into()
}

#[marine]
pub fn get_pending_transactions() -> FdbTransactionsResult {
    wrapped_try(|| get_storage()?.get_pending_transactions()).into()
}

#[marine]
pub fn get_active_crons() -> FdbCronsResult {
    wrapped_try(|| get_storage()?.get_active_crons()).into()
}

#[marine]
pub fn get_all_crons() -> FdbCronsResult {
    wrapped_try(|| get_storage()?.get_all_crons()).into()
}

#[marine]
pub fn get_metadata_with_history(
    data_key: String,
    public_key: String,
    alias: String,
) -> FdbMetadataHistoryResult {
    wrapped_try(|| {
        let storage = get_storage().expect("Internal error to database connector");

        let result = storage.get_owner_metadata_by_datakey_and_alias(data_key, public_key, alias);

        let metadata;
        let mut metadatas: Vec<String> = Vec::new();

        match result {
            Ok(m) => {
                metadata = m;
            }
            Err(e) => return Err(e),
        };

        let mut read_metadata_cid: String = metadata.cid.clone();

        while read_metadata_cid.len() > 0 {
            let result = get(read_metadata_cid.clone(), "".to_string(), 0);
            let val: Value = serde_json::from_str(&result.block).unwrap();

            let input = format!(r#"{}"#, val);
            metadatas.push(input);

            let previous_cid = val
                .get("previous")
                .and_then(|v| v.get("/"))
                .and_then(|v| v.as_str());

            if previous_cid.is_none() {
                break;
            } else {
                read_metadata_cid = previous_cid.unwrap().to_string();
            }
        }

        Ok(metadatas)
    })
    .into()
}

// *********** VALIDATOR *****************
#[marine]
pub fn bind_meta_contract(transaction_hash: String) {
    validate_meta_contract(transaction_hash);
}

#[marine]
pub fn set_metadata(
    transaction_hash: String,
    meta_contract_id: String,
    on_metacontract_result: bool,
    metadatas: Vec<FinalMetadata>,
    final_error_msg: String,
) {
    validate_metadata(
        transaction_hash,
        meta_contract_id,
        on_metacontract_result,
        metadatas,
        final_error_msg,
    );
}

#[marine]
pub fn set_clone(
    transaction_hash: String,
    meta_contract_id: String,
    on_metacontract_result: bool,
    data: String,
    final_error_msg: String,
) {
    validate_clone(
        transaction_hash,
        meta_contract_id,
        on_metacontract_result,
        data,
        final_error_msg,
    );
}

/************************ *********************/
#[marine]
#[link(wasm_import_module = "ipfsdag")]
extern "C" {
    #[link_name = "put_block"]
    pub fn put_block(
        content: String,
        previous_cid: String,
        transaction: String,
        api_multiaddr: String,
        timeout_sec: u64,
    ) -> IpfsDagPutResult;

    #[link_name = "get"]
    pub fn get(hash: String, api_multiaddr: String, timeout_sec: u64) -> IpfsDagGetResult;
}

#[marine]
#[link(wasm_import_module = "crypto")]
extern "C" {
    #[link_name = "verify"]
    pub fn verify(public_key: String, signature: String, message: String, enc: String) -> bool;

    #[link_name = "get_public_key_type"]
    pub fn get_public_key_type(public_key: &str) -> String;
}
