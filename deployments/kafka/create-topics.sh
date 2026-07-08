#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BROKERS:-kafka:9092}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-6}"
RETENTION_MS="${KAFKA_TOPIC_RETENTION_MS:-259200000}"

create_topic() {
  local topic="$1"

  /opt/bitnami/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor 1 \
    --config "retention.ms=${RETENTION_MS}"
}

create_topic "${KAFKA_LOG_TOPIC:-logs.raw}"
create_topic "${KAFKA_RETRY_TOPIC:-logs.retry}"
create_topic "${KAFKA_DLQ_TOPIC:-logs.dlq}"

/opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --list
