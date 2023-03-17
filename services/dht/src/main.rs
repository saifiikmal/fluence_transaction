#![allow(improper_ctypes)]

mod defaults;
mod error;
mod result;
mod storage_impl;
mod transaction;
pub mod transactions_impl;

use marine_rs_sdk::marine;
use marine_rs_sdk::module_manifest;
use marine_rs_sdk::WasmLoggerBuilder;

use error::ServiceError::InvalidSignature;
use result::FdbResult;
use result::FdbTransactionsResult;
use std::time::{SystemTime, UNIX_EPOCH};
use storage_impl::get_storage;
use transaction::Transaction;
use types::{IpfsDagGetResult, IpfsDagPutResult};

#[macro_use]
extern crate fstrings;

module_manifest!();

const DEFAULT_ENC: &str = "secp256k1";

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
}

#[marine]
pub fn send_transaction(
    data_key: String,
    alias: String,
    public_key: String,
    signature: String,
    metadata: String,
    enc: String,
) -> FdbResult {
    wrapped_try(|| {
        let v = verify(
            public_key.clone(),
            signature.clone(),
            metadata.clone(),
            enc.clone(),
        );

        if !v {
            return Err(InvalidSignature(f!("not owner of data_key: {public_key}")));
        }

        let enc_verify;

        if enc.is_empty() || enc == DEFAULT_ENC {
            enc_verify = DEFAULT_ENC.to_string();
        } else {
            enc_verify = enc;
        }

        let cp = marine_rs_sdk::get_call_parameters();

        let result = put(metadata.clone(), "".to_string(), "".to_string(), 0);

        let now = SystemTime::now();
        let timestamp = now.duration_since(UNIX_EPOCH).expect("Time went backwards");

        let transaction = Transaction::new(
            cp.init_peer_id,
            cp.host_id,
            data_key,
            metadata,
            signature,
            public_key,
            alias,
            timestamp.as_millis() as u64,
            enc_verify,
            result.cid,
        );

        let storage = get_storage()?;
        storage.write(transaction)
    })
    .into()
}

#[marine]
pub fn get_transactions(data_key: String) -> FdbTransactionsResult {
    wrapped_try(|| get_storage()?.get_transactions_by_datakey(data_key)).into()
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
