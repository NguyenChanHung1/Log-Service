#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-deployments/docker-compose.yml}"

echo "Docker Compose services:"
docker compose -f "${COMPOSE_FILE}" ps

echo
echo "Kafka topics:"
docker compose -f "${COMPOSE_FILE}" exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

echo
echo "Elasticsearch health:"
curl -fsS "http://localhost:9200/_cluster/health?pretty"

echo
echo "Elasticsearch logs template:"
curl -fsS "http://localhost:9200/_index_template/logs-template?pretty"
