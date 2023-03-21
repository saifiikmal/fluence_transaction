#![allow(improper_ctypes)]

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

use defaults::{ENCRYPTION_TYPE_ED25519, ENCRYPTION_TYPE_SECP256K1, STATUS_SUCCESS};
use defaults::{METHOD_CREATE, METHOD_UPDATE, STATUS_FAILED};
use marine_rs_sdk::marine;
use marine_rs_sdk::module_manifest;
use marine_rs_sdk::WasmLoggerBuilder;

use error::ServiceError::{
    self, InvalidMethod, InvalidOwner, InvalidSignature, NoEncryptionType,
    NotSupportedEncryptionType,
};
use meta_contract::MetaContract;
use metadatas::Metadata;
use result::{FdbMetaContractResult, FdbTransactionResult};
use result::{FdbMetadataResult, FdbResult};
use std::time::{SystemTime, UNIX_EPOCH};
use storage_impl::get_storage;
use transaction::Transaction;
use types::{IpfsDagGetResult, IpfsDagPutResult};

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
    storage.create_meta_contract_tables();
    storage.create_transactions_tables();
    storage.create_metadatas_tables();
}

#[marine]
pub fn send_transaction(
    data_key: String,
    token_key: String,
    alias: String,
    public_key: String,
    signature: String,
    data: String,
    enc: String,
    method: String,
    nonce: i64,
) -> FdbResult {
    let mut service_id = "".to_string();
    let mut error: Option<ServiceError> = None;
    let mut enc_verify = "".to_string();
    let storage = get_storage().expect("Database non existance");

    if error.is_none() {
        if method != METHOD_CREATE && method != METHOD_UPDATE {
            error = Some(InvalidMethod(f!("invalid method: {method}")));
        }
    }

    if error.is_none() {
        if method == METHOD_UPDATE {
            let result = storage.get_metadata(data_key.clone());
            match result {
                Ok(metadata) => {
                    if metadata.public_key != public_key {
                        error = Some(InvalidOwner(f!("not owner of data_key: {public_key}")));
                    }

                    enc_verify = metadata.enc;
                    service_id = metadata.service_id;
                }
                Err(e) => error = Some(e),
            }
        } else if method == METHOD_CREATE {
            enc_verify = enc.clone();
            service_id = data.clone();
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
        enc_verify,
        service_id,
        method,
    );

    if !error.is_none() {
        transaction.error_text = error.unwrap().to_string();
        transaction.status = STATUS_FAILED;
    }

    log::info!("{:?}", transaction);
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
pub fn get_metadata(data_key: String) -> FdbMetadataResult {
    wrapped_try(|| get_storage()?.get_metadata(data_key)).into()
}

#[marine]
pub fn get_meta_contract(token_key: String) -> FdbMetaContractResult {
    wrapped_try(|| get_storage()?.get_meta_contract(token_key)).into()
}

// *********** SMART CONTRACT *****************
#[marine]
pub fn bind_meta_contract(transaction_hash: String) {
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
                current_meta_contract.service_id = transaction.data.clone();
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
                service_id: transaction.service_id.clone(),
                public_key: transaction.public_key.clone(),
            };

            meta_result = storage.write_meta_contract(current_meta_contract);
        } else {
            meta_result = storage
                .rebind_meta_contract(transaction.token_key.clone(), transaction.data.clone());
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

// *********** VALIDATOR *****************

#[marine]
pub fn create_metadata(
    transaction_hash: String,
    on_create_result: bool,
    on_create_metadata: String,
    on_create_error_msg: String,
) {
    let storage = get_storage().expect("Internal error to database connector");
    let mut transaction = storage.get_transaction(transaction_hash).unwrap();

    if !on_create_result {
        transaction.status = STATUS_FAILED;
        transaction.error_text = on_create_error_msg;
    } else {
        let mut content_cid = "".to_string();

        if !on_create_metadata.is_empty() {
            let result = put(on_create_metadata, "".to_string(), "".to_string(), 0);
            content_cid = result.cid
        }

        let metadata = Metadata::new(
            transaction.data_key.clone(),
            transaction.alias.clone(),
            content_cid,
            transaction.public_key.clone(),
            transaction.encryption_type.clone(),
            transaction.data.clone(), // service_id is stored in metadata
        );

        let _ = storage.write_metadata(metadata);

        transaction.status = STATUS_SUCCESS;
    }

    // update transaction
    let _ = storage.write_transaction(transaction);
}

#[marine]
pub fn update_metadata(
    transaction_hash: String,
    on_update_result: bool,
    on_update_metadata: String,
    on_update_error_msg: String,
) {
    let storage = get_storage().expect("Internal error to database connector");
    let mut transaction = storage.get_transaction(transaction_hash).unwrap().clone();

    if !on_update_result {
        transaction.status = STATUS_FAILED;
        transaction.error_text = on_update_error_msg;
    } else {
        let result = storage.get_metadata(transaction.data_key.clone());

        let result_ipfs_dag_put = put(on_update_metadata, "".to_string(), "".to_string(), 0);
        let content_cid = result_ipfs_dag_put.cid;

        match result {
            Ok(metadata) => {
                let mut meta = metadata;
                meta.cid = content_cid;
                let _ = storage.write_metadata(meta);

                transaction.status = STATUS_SUCCESS;
            }
            Err(e) => {
                transaction.error_text = e.to_string();
                transaction.status = STATUS_FAILED;
            }
        };
    }

    // update transaction
    let _ = storage.write_transaction(transaction);
}

/************************ *********************/
#[marine]
#[link(wasm_import_module = "ipfsdag")]
extern "C" {
    #[link_name = "put"]
    pub fn put(
        object: String,
        previous_cid: String,
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
}
