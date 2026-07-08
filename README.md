# Log Service

Log Service is a high-throughput log ingestion project designed to receive logs from multiple sources, buffer them safely, process them concurrently, and store them for search and monitoring.

The target stack is:

- Go services for ingestion, workers, generator, and dashboard API
- Kafka in KRaft mode for durable buffering
- Elasticsearch for indexed log storage
- Docker Compose for local deployment
- A lightweight dashboard UI for operational visibility

Prometheus is intentionally not used. Monitoring is handled through service health endpoints, JSON stats endpoints, Docker stats, Kafka CLI checks, Elasticsearch APIs, and the project dashboard.

## Current Status

Implemented so far:

- Project skeleton with Go module, Dockerfile, Makefile, and environment defaults
- Log parser for the required format:

  ```text
  <timestamp> <ip> <method> <path> <status>
  ```

- Docker Compose infrastructure for Kafka and Elasticsearch
- Kafka topic initialization for `logs.raw`, `logs.retry`, and `logs.dlq`
- Elasticsearch index template for `logs-*`
- Placeholder dashboard API and dashboard UI
- HTTP log ingestion endpoint at `POST /v1/logs`
- Kafka producer integration for accepted log batches
- Durable local spool fallback when Kafka publishing fails
- Graceful shutdown for the log API

Planned next:

- Worker consumer and Elasticsearch bulk writer
- Spool replay behavior
- Full dashboard with CPU/RAM, filters, and real-time logs

## Repository Layout

```text
cmd/
  dashboard-api/    Placeholder dashboard API
  log-api/          Log ingestion API skeleton
  log-generator/    Log generator skeleton
  log-worker/       Worker skeleton
deployments/
  docker-compose.yml
  elasticsearch/
  kafka/
internal/
  config/           Environment config loader
  logline/          Log parser and validation
requirements/       Architecture and implementation planning docs
scripts/            Local operational scripts
web/dashboard/      Placeholder dashboard UI
```

## Prerequisites

- Go 1.22+
- Docker
- Docker Compose v2
- curl

## Configuration

Copy the sample environment file if you want a local editable config:

```bash
cp .env.example .env
```

Important defaults:

```text
APP_PORT=8080
WORKER_PORT=8081
DASHBOARD_API_PORT=8082
DASHBOARD_UI_PORT=3000
KAFKA_BROKERS=localhost:29092
ELASTICSEARCH_URL=http://localhost:9200
```

Inside Docker Compose, services use internal addresses such as `kafka:9092` and `http://elasticsearch:9200`.

## Build

Build all Go binaries:

```bash
make build
```

Generated binaries are written to `bin/`.

## Test

Run all Go tests:

```bash
go test ./...
```

Validate Docker Compose configuration:

```bash
docker compose -f deployments/docker-compose.yml config
docker compose -f deployments/docker-compose.yml --profile app config
```

## Local Deployment

Start Kafka and Elasticsearch:

```bash
docker compose -f deployments/docker-compose.yml up
```

Start Kafka, Elasticsearch, dashboard API, and dashboard UI:

```bash
docker compose -f deployments/docker-compose.yml --profile app up --build
```

Run in detached mode:

```bash
docker compose -f deployments/docker-compose.yml --profile app up --build -d
```

Check running services:

```bash
docker compose -f deployments/docker-compose.yml ps
```

Stop services:

```bash
docker compose -f deployments/docker-compose.yml --profile app down
```

## Service URLs

When the app profile is running:

- Dashboard UI: `http://localhost:3000`
- Dashboard API health: `http://localhost:8082/healthz`
- Elasticsearch: `http://localhost:9200`
- Kafka external listener: `localhost:29092`

Current Go skeleton endpoints:

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
curl http://localhost:8081/healthz
curl http://localhost:8082/healthz
curl http://localhost:8082/api/overview
```

Submit a test log batch:

```bash
curl -i -X POST http://localhost:8080/v1/logs \
  -H "Content-Type: application/json" \
  -d '{"source":"manual-test","records":["2026-07-07T09:00:01Z 10.10.1.5 GET /login 200"]}'
```

## Infrastructure Checks

After starting Compose, run:

```bash
make check-system
```

Equivalent manual checks:

```bash
docker compose -f deployments/docker-compose.yml ps
curl -fsS "http://localhost:9200/_cluster/health?pretty"
curl -fsS "http://localhost:9200/_index_template/logs-template?pretty"
docker compose -f deployments/docker-compose.yml exec kafka \
  /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

## Development Verification Steps

After making code changes, run these steps:

```bash
gofmt -w $(find cmd internal -name "*.go")
go test ./...
make build
docker compose -f deployments/docker-compose.yml config
docker compose -f deployments/docker-compose.yml --profile app config
```

If your change touches Docker services, also run:

```bash
docker compose -f deployments/docker-compose.yml --profile app up --build
make check-system
docker compose -f deployments/docker-compose.yml --profile app down
```

## Architecture Summary

The planned full workflow is:

1. Log generator creates logs at configurable TPS.
2. Log API validates batches and durably accepts logs.
3. Kafka stores accepted logs in `logs.raw`.
4. Worker consumes Kafka records, parses them, and writes to Elasticsearch.
5. Invalid or poison records go to `logs.dlq`.
6. Temporary failures are retried or replayed from durable storage.
7. Dashboard displays service status, CPU/RAM, request filters, Kafka lag, Elasticsearch status, and real-time logs.

For more detail, see:

- [requirements/log-processing-system-plan.md](requirements/log-processing-system-plan.md)
- [requirements/implementation-plan.md](requirements/implementation-plan.md)
- [requirements/full-workflow-diagram.md](requirements/full-workflow-diagram.md)

## Reliability Notes

Accepted logs should not be dropped. The design uses Kafka as the primary durable buffer. If Kafka is temporarily saturated or unavailable, the planned API behavior is to write accepted records to a durable local spool and replay them later. `503 Service Unavailable` should only be returned when no durable write path is available.

Elasticsearch write failures should not drop input data. The worker should retry temporary failures, pause or slow consumption during outages, and use durable replay storage or retry topics for prolonged failures.
