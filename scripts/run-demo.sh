#!/usr/bin/env bash
set -euo pipefail

TARGET="${TARGET:-http://localhost:8080/v1/logs}"
CLIENTS="${CLIENTS:-4}"
TPS="${TPS:-500}"
DURATION="${DURATION:-30s}"
BATCH_SIZE="${BATCH_SIZE:-100}"
MODE="${MODE:-steady}"

echo "Checking service readiness..."
curl -fsS http://localhost:8080/readyz >/dev/null
curl -fsS http://localhost:8081/readyz >/dev/null

echo "Running generator: clients=${CLIENTS} tps=${TPS} duration=${DURATION} batch_size=${BATCH_SIZE} mode=${MODE}"
./bin/log-generator \
  --target "${TARGET}" \
  --clients "${CLIENTS}" \
  --tps "${TPS}" \
  --duration "${DURATION}" \
  --batch-size "${BATCH_SIZE}" \
  --mode "${MODE}"

echo
echo "API stats:"
curl -fsS http://localhost:8080/stats
echo
echo "Worker stats:"
curl -fsS http://localhost:8081/stats
echo
echo "Elasticsearch count:"
curl -fsS "http://localhost:9200/logs-*/_count?pretty"
