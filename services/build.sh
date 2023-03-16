#!/usr/bin/env bash
set -o errexit -o nounset -o pipefail

# set current working directory to script directory to run script from everywhere
cd "$(dirname "$0")"

# This script builds all subprojects and puts all created Wasm modules in one dir
echo "compiling crypto..."
cd crypto
cargo update --aggressive
marine build --release

echo "compiling ipfsdag..."
cd ../ipfsdag
cargo update --aggressive
marine build --release

echo "compiling dht..."
cd ../dht
cargo update --aggressive
marine build --release


cd ..
mkdir -p artifacts
rm -f artifacts/*.wasm
cp target/wasm32-wasi/release/crypto.wasm artifacts/
cp target/wasm32-wasi/release/ipfsdag.wasm artifacts/
cp target/wasm32-wasi/release/dht.wasm artifacts/
marine aqua artifacts/dht.wasm -s Dht -i dht > ../aqua/dht.aqua

wget https://github.com/fluencelabs/sqlite/releases/download/v0.18.0_w/sqlite3.wasm
mv sqlite3.wasm artifacts/

RUST_LOG="info" mrepl --quiet Config.toml