#! /bin/bash

# aqua script add \
# --sk $1 \
# --input ../aqua/validator.aqua \
# --func 'getPendingTransactions_3600()' \
# --addr $2 \
# --interval 1

aqua run --input ../aqua/validator.aqua --func 'validateTransaction("myDm3yj56tirdCDag9tm7P3rU38fxt4Bwz1Dm97NYuk")' --addr /ip4/127.0.0.1/tcp/9991/ws/p2p/12D3KooWHBG9oaVx4i3vi6c1rSBUm7MLBmyGmmbHoZ23pmjDCnvK