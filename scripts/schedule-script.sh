#! /bin/bash

# aqua script add \
# --sk $1 \
# --input ../aqua/validator.aqua \
# --func 'getPendingTransactions_3600()' \
# --addr $2 \
# --interval 1

aqua run --input ../aqua/validator.aqua --func 'validateTransaction("3wZbnvuLK289kEHbceDGWSabMnuaJBJabEqWirCczGxY")' --addr /ip4/127.0.0.1/tcp/9992/ws/p2p/12D3KooWRABanQHUn28dxavN9ZS1zZghqoZVAYtFpoN7FdtoGTFv