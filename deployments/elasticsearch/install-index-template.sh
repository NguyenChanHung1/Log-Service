#!/usr/bin/env sh
set -eu

ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-http://elasticsearch:9200}"

curl -fsS \
  -X PUT \
  "${ELASTICSEARCH_URL}/_index_template/logs-template" \
  -H "Content-Type: application/json" \
  --data-binary "@/templates/index-template.json"

echo
echo "installed logs-template"
