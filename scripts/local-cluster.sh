#!/usr/bin/env bash
# Starts a 3-node cluster on localhost:9001-9003. Ctrl-C stops all nodes.
set -euo pipefail
cd "$(dirname "$0")/.."
go build -o ./kvnode ./cmd/kvnode
PEERS=http://127.0.0.1:9001,http://127.0.0.1:9002,http://127.0.0.1:9003
for i in 0 1 2; do ./kvnode -id $i -peers $PEERS -data ./data/$i & done
trap 'kill $(jobs -p) 2>/dev/null' EXIT
wait
