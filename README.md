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
- Dashboard API and static dashboard UI for overview, filters, stored logs, and real-time log stream
- Worker consumer for `logs.raw` with Elasticsearch bulk indexing
- Worker retry, DLQ, and durable replay spool for Elasticsearch failures
- Concurrent log generator with configurable clients, TPS, duration, batch size, and traffic mode
- HTTP log ingestion endpoint at `POST /v1/logs`
- Kafka producer integration for accepted log batches
- Durable local spool fallback when Kafka publishing fails
- API overload modes: normal, degraded, critical, exhausted
- API spool replay back into Kafka after broker recovery
- Demo and evidence-capture scripts for final reporting
- Graceful shutdown for the log API

Planned next:

- Final tuning and screenshots for the submitted report

## Repository Layout

```text
cmd/
  dashboard-api/    Dashboard query and monitoring API
  log-api/          Log ingestion API skeleton
  log-generator/    Concurrent load generator
  log-worker/       Kafka consumer and Elasticsearch writer
deployments/
  docker-compose.yml
  elasticsearch/
  kafka/
internal/
  config/           Environment config loader
  dashboard/        Dashboard API collectors and handlers
  ingestion/        HTTP log ingestion
  logline/          Log parser and validation
  metrics/          Runtime and ingestion counters
requirements/       Architecture and implementation planning docs
scripts/            Local operational scripts
web/dashboard/      Static dashboard UI
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
LOG_API_URL=http://localhost:8080
WORKER_API_URL=http://localhost:8081
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

Useful local endpoints:

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
curl http://localhost:8080/stats
curl http://localhost:8081/healthz
curl http://localhost:8081/readyz
curl http://localhost:8081/stats
curl http://localhost:8082/healthz
curl http://localhost:8082/api/overview
curl "http://localhost:8082/api/metrics"
curl "http://localhost:8082/api/logs?limit=10"
```

Submit a test log batch:

```bash
curl -i -X POST http://localhost:8080/v1/logs \
  -H "Content-Type: application/json" \
  -d '{"source":"manual-test","records":["2026-07-07T09:00:01Z 10.10.1.5 GET /login 200"]}'
```

Run the log generator:

```bash
./bin/log-generator --target http://localhost:8080/v1/logs --clients 4 --tps 500 --duration 30s --batch-size 100 --mode steady
./bin/log-generator --target http://localhost:8080/v1/logs --clients 4 --tps 1000 --duration 30s --batch-size 100 --mode burst
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
4. Worker consumes Kafka records, validates them, bulk-indexes them into Elasticsearch, and commits offsets only after safe handling.
5. Invalid or poison records go to `logs.dlq` with reason metadata.
6. Temporary Elasticsearch failures are retried and then written to a durable worker replay spool for later indexing.
7. Dashboard displays service status, CPU/RAM, request filters, Kafka depth, Elasticsearch status, stored logs, and real-time logs.

For more detail, see:

- [requirements/log-processing-system-plan.md](requirements/log-processing-system-plan.md)
- [requirements/implementation-plan.md](requirements/implementation-plan.md)
- [requirements/full-workflow-diagram.md](requirements/full-workflow-diagram.md)
- [docs/architecture.md](docs/architecture.md)
- [docs/operations.md](docs/operations.md)
- [docs/test-report.md](docs/test-report.md)

## Reliability Notes

Accepted logs should not be dropped. Kafka is the primary durable buffer. If Kafka is temporarily saturated or unavailable, the API writes accepted records to a durable local spool and replays them later. `503 Service Unavailable` with `Retry-After` is returned only when the short in-flight handoff is saturated or no durable write path remains.

Elasticsearch write failures should not drop input data. The worker should retry temporary failures, pause or slow consumption during outages, and use durable replay storage or retry topics for prolonged failures.
