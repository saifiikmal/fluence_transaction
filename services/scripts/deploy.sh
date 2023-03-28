#! /bin/bash

aqua remote deploy_service \
--addr $1 \
--sk $2 \
--config-path configs/deploy-cfg.json \
--service zero