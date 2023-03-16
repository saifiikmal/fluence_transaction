#![allow(improper_ctypes)]

mod ed25519;
mod secp256k1;

use ed25519_compact::KeyPair;
use ed25519_compact::{Noise, SecretKey};
use marine_rs_sdk::marine;
use marine_rs_sdk::module_manifest;
use marine_rs_sdk::WasmLoggerBuilder;
use std::ops::Deref;

use ed25519::verify as verify_ed25519;
use secp256k1::verify as verify_secp256k1;

const DEFAULT_ENC: &str = "secp256k1";

use types::*;

module_manifest!();

pub fn main() {
    WasmLoggerBuilder::new()
        .with_log_level(log::LevelFilter::Info)
        .build()
        .unwrap();
}

#[marine]
pub fn generate_keypair() -> Ed25519KeyPair {
    let kp = KeyPair::generate();
    let base64_pk = base64::encode(kp.pk.deref());

    let base64_sk = base64::encode(kp.sk.deref());

    Ed25519KeyPair {
        pk: base64_pk,
        sk: base64_sk,
    }
}

#[marine]
pub fn verify(public_key: String, signature: String, message: String, enc: String) -> bool {
    let verify: bool;

    if enc.is_empty() || enc == DEFAULT_ENC {
        verify = verify_secp256k1(public_key.clone(), signature.clone(), message);
    } else {
        verify = verify_ed25519(public_key.clone(), signature.clone(), message);
    }

    verify
}

#[marine]
pub fn sign(message: String, private_key: String) -> String {
    let pk_key_decoded = base64::decode(private_key).unwrap();

    let pk = pk_key_decoded.try_into().expect("invalid private key");

    let sk = SecretKey::new(pk);

    let signature = sk.sign(message, Some(Noise::default()));

    base64::encode(signature)
}
