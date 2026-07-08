#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${OUT_DIR:-docs/evidence}"
mkdir -p "${OUT_DIR}"

{
  date -Is
  echo
  docker stats --no-stream || true
  echo
  curl -fsS http://localhost:8080/stats || true
  echo
  curl -fsS http://localhost:8081/stats || true
  echo
  curl -fsS "http://localhost:9200/logs-*/_count?pretty" || true
  echo
  curl -fsS "http://localhost:8082/api/logs?limit=5" || true
} > "${OUT_DIR}/running.txt"

echo "Wrote ${OUT_DIR}/running.txt"
