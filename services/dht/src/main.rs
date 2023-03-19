#![allow(improper_ctypes)]

mod defaults;
mod error;
mod metadatas;
mod metadatas_impl;
mod result;
mod storage_impl;
mod transaction;
pub mod transactions_impl;

use defaults::{ENCRYPTION_TYPE_ED25519, ENCRYPTION_TYPE_SCP256K1};
use defaults::{METHOD_UPDATE, STATUS_FAILED};
use marine_rs_sdk::marine;
use marine_rs_sdk::module_manifest;
use marine_rs_sdk::WasmLoggerBuilder;

use error::ServiceError::{
    self, InternalError, InvalidOwner, InvalidSignature, NotSupportedEncryptionType,
};
use metadatas::Metadata;
use result::{FdbMetadataResult, FdbResult};
use result::{FdbTransactionResult, FdbTransactionsResult};
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
    storage.create_transactions_tables();
    storage.create_metadatas_tables();
}

#[marine]
pub fn send_transaction(
    data_key: String,
    alias: String,
    public_key: String,
    signature: String,
    metadata: String,
    enc: String,
    method: String,
) -> FdbResult {
    let mut error: Option<ServiceError> = None;
    let mut cid: String = "".to_string();
    let mut enc_verify = ENCRYPTION_TYPE_SCP256K1.to_string();
    let current_metadata;
    let storage = get_storage().expect("Database non existance");

    if !error.is_none() {
        if method == METHOD_UPDATE {
            current_metadata = storage.get_metadata(data_key.clone()).unwrap();
            if current_metadata.public_key != public_key {
                error = Some(InvalidOwner(f!("not owner of data_key: {public_key}")));
            }

            enc_verify = current_metadata.enc;
        } else {
            if enc.clone().is_empty()
                && enc.clone() != ENCRYPTION_TYPE_SCP256K1
                && enc.clone() != ENCRYPTION_TYPE_ED25519
            {
                error = Some(NotSupportedEncryptionType(enc.clone()));
            }

            enc_verify = enc.clone();
        }
    }

    if !error.is_none() {
        let v = verify(
            public_key.clone(),
            signature.clone(),
            metadata.clone(),
            enc_verify.clone(),
        );

        if !v {
            error = Some(InvalidSignature(f!("not owner of data_key: {public_key}")));
        }
    }

    let cp = marine_rs_sdk::get_call_parameters();

    if method == METHOD_UPDATE {
        let result = put(metadata.clone(), "".to_string(), "".to_string(), 0);
        cid = result.cid
    }

    let now = SystemTime::now();
    let timestamp = now.duration_since(UNIX_EPOCH).expect("Time went backwards");

    let mut transaction = Transaction::new(
        cp.init_peer_id,
        cp.host_id,
        data_key,
        metadata,
        public_key,
        alias,
        timestamp.as_millis() as u64,
        enc_verify,
        cid,
        method,
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
pub fn get_metadata(data_key: String) -> FdbMetadataResult {
    wrapped_try(|| get_storage()?.get_metadata(data_key)).into()
}

// *********** VALIDATOR *****************

#[marine]
pub fn create_metadata(transaction_hash: String) {
    let storage = get_storage().expect("Internal error to database connector");
    let transaction = storage.get_transaction(transaction_hash).unwrap();

    let metadata = Metadata::new(
        transaction.data_key,
        transaction.alias,
        "".to_string(),
        transaction.public_key,
        transaction.encryption_type,
        transaction.metadata,
    );

    storage.write_metadata(metadata);
}

#[marine]
pub fn update_metadata(data_key: String, alias: String, public_key: String, service_id: String) {}

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
