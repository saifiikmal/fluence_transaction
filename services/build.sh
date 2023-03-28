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

echo "compiling node..."
cd ../node
cargo update --aggressive
marine build --release


cd ..
mkdir -p artifacts
rm -f artifacts/*.wasm
cp target/wasm32-wasi/release/crypto.wasm artifacts/
cp target/wasm32-wasi/release/ipfsdag.wasm artifacts/
cp target/wasm32-wasi/release/node.wasm artifacts/
marine aqua artifacts/node.wasm -s Node -i node > ../aqua/node.aqua

wget https://github.com/fluencelabs/sqlite/releases/download/v0.18.0_w/sqlite3.wasm
mv sqlite3.wasm artifacts/

RUST_LOG="info" mrepl --quiet Config.toml