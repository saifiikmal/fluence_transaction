#![allow(improper_ctypes)]

use base58::FromBase58;
use ed25519_compact::{PublicKey, Signature};

pub fn verify(public_key: String, signature: String, message: String) -> bool {
    let p_key_decoded = public_key.clone().from_base58().unwrap();
    let sign_decoded = signature.clone().from_base58().unwrap();

    let pk: [u8; 32] = p_key_decoded
        .try_into()
        .expect("Error: public_key with incorrect length");

    let sign: [u8; 64] = sign_decoded
        .try_into()
        .expect("Error: Sign with incorrect length");

    let p_key = PublicKey::new(pk);

    match p_key.verify(message, &Signature::new(sign)) {
        Ok(_) => true,
        Err(_) => false,
    }
}
