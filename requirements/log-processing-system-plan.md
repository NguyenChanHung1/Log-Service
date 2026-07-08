# Log Processing System Architecture And Delivery Plan

## 1. Project Goal

Build a scalable log ingestion platform that can receive logs from many concurrent sources, buffer traffic spikes, process records concurrently, and store searchable log data efficiently.

The preferred stack is:

- Go for the log generator, ingestion API, and background consumers
- Kafka for durable buffering, partitioned throughput, retry isolation, and backpressure
- Elasticsearch for indexed log storage and querying
- Docker Compose for local development, integration testing, and demo execution

Prometheus is intentionally not included. Operational monitoring will use application health endpoints, Go runtime metrics exposed through HTTP/JSON, Docker stats, Kafka consumer lag tooling, Elasticsearch health APIs, and structured service logs.

## 2. Component Architecture Overview

```text
+---------------------+        +---------------------+        +------------------+
| Log Generator       |        | Log Processing API  |        | Kafka            |
|                     | HTTP   |                     | Produce|                  |
| - configurable TPS  +------->+ - validate logs     +------->+ logs.raw topic   |
| - concurrent clients|        | - batch ingestion   |        | DLQ topic        |
| - realistic payloads|        | - rate limiting     |        | retry topics     |
+---------------------+        | - health endpoints  |        +--------+---------+
                               +---------------------+                 |
                                                                       | Consume
                                                                       v
                               +---------------------+        +------------------+
                               | Log Worker Service  | Bulk   | Elasticsearch    |
                               |                     +------->+                  |
                               | - consumer group    | Index  | logs-* indices   |
                               | - parse/enrich      |        | templates/ILM    |
                               | - bulk writer       |        +------------------+
                               | - retry/DLQ         |
                               +----------+----------+
                                          |
                                          v
                               +---------------------+
                               | Monitoring Surface  |
                               |                     |
                               | - /healthz          |
                               | - /readyz           |
                               | - /metrics JSON     |
                               | - Docker stats      |
                               | - Kafka lag checks  |
                               | - ES cluster health |
                               +---------------------+
```

## 3. Component Responsibilities

### 3.1. Log Generator

Purpose: simulate multiple independent clients sending logs continuously to the ingestion API.

Responsibilities:

- Generate records in the required format:

  ```text
  <timestamp> <ip> <method> <path> <status>
  ```

- Support configurable TPS, duration, client count, batch size, and target URL.
- Support both steady traffic and burst traffic modes.
- Send logs over HTTP using JSON batches to reduce request overhead.
- Print execution statistics: total generated, successful sends, failed sends, retry count, average latency, p95 latency, and effective TPS.

Example command:

```bash
go run ./cmd/log-generator \
  --target http://localhost:8080/v1/logs \
  --clients 20 \
  --tps 5000 \
  --duration 10m \
  --batch-size 250
```

### 3.2. Log Processing API

Purpose: receive log batches from multiple concurrent clients and safely enqueue them for downstream processing.

Responsibilities:

- Expose `POST /v1/logs` for batched ingestion.
- Validate payload shape, batch size, and log line format.
- Reject invalid records with clear error responses.
- Publish accepted records to Kafka topic `logs.raw`.
- Apply request timeout, body size limit, and basic rate limiting.
- Return `202 Accepted` only after Kafka acknowledgement.
- Expose:
  - `GET /healthz` for process liveness
  - `GET /readyz` for Kafka connectivity readiness
  - `GET /stats` for JSON runtime and ingestion counters

Initial API payload:

```json
{
  "source": "client-001",
  "records": [
    "2026-07-07T09:00:01Z 10.10.1.5 GET /login 200",
    "2026-07-07T09:00:02Z 10.10.1.6 POST /payment 500"
  ]
}
```

### 3.3. Kafka

Purpose: decouple ingestion from storage and absorb bursts when incoming traffic exceeds processing capacity.

Topics:

- `logs.raw`: accepted raw log records
- `logs.retry`: records that failed due to temporary downstream errors
- `logs.dlq`: records that repeatedly fail parsing or indexing

Recommended local settings:

- `logs.raw` partitions: 6 to 12
- replication factor: 1 for local Compose, 3 for production-like deployment
- retention: 1 to 3 days locally, adjusted by storage budget in production
- compression: producer-side `snappy` or `lz4`

Backpressure behavior:

- If Kafka is healthy but workers lag, API continues accepting until Kafka retention/storage limits are approached.
- If Kafka produce latency becomes high, API returns `429 Too Many Requests` or `503 Service Unavailable` depending on failure cause.
- If Kafka is unavailable, API readiness fails and ingestion requests return `503`.

### 3.4. Log Worker Service

Purpose: consume Kafka records, parse them into structured documents, and write to Elasticsearch efficiently.

Responsibilities:

- Run as a Kafka consumer group so worker replicas can scale horizontally.
- Parse required fields: timestamp, IP, method, path, status.
- Add metadata: source, received timestamp, processed timestamp, worker ID.
- Batch records into Elasticsearch bulk requests.
- Retry temporary Elasticsearch failures with exponential backoff.
- Send permanently invalid records to `logs.dlq`.
- Commit Kafka offsets only after successful indexing or DLQ handling.
- Expose:
  - `GET /healthz`
  - `GET /readyz`
  - `GET /stats`

Structured Elasticsearch document example:

```json
{
  "@timestamp": "2026-07-07T09:00:01Z",
  "source": "client-001",
  "ip": "10.10.1.5",
  "method": "GET",
  "path": "/login",
  "status": 200,
  "received_at": "2026-07-07T09:00:01.120Z",
  "processed_at": "2026-07-07T09:00:01.260Z",
  "raw": "2026-07-07T09:00:01Z 10.10.1.5 GET /login 200"
}
```

### 3.5. Elasticsearch

Purpose: provide efficient indexed storage for high-volume log records.

Index strategy:

- Use daily indices such as `logs-2026.07.07`.
- Define an index template for stable mappings.
- Store IP as `ip`, status as `integer`, timestamp fields as `date`, and method/path/source as `keyword`.
- Use bulk indexing from workers.
- For the two-week project, use local single-node Elasticsearch in Docker Compose.

Future production enhancements:

- Index Lifecycle Management for rollover and retention.
- Hot/warm/cold node tiers if data volume grows.
- Replica shards and multi-node Elasticsearch cluster.

## 4. Data Processing Flow

1. The log generator creates log lines at the configured TPS across multiple concurrent clients.
2. Each generator client sends batches to `POST /v1/logs`.
3. The Log Processing API validates request size and record format.
4. Valid records are published to Kafka topic `logs.raw`.
5. API returns `202 Accepted` after Kafka acknowledges the write.
6. Worker service consumes records from Kafka using a shared consumer group.
7. Worker parses records and converts raw lines into structured documents.
8. Worker writes documents to Elasticsearch using the bulk API.
9. Worker commits Kafka offsets after successful storage.
10. Invalid or repeatedly failing records are sent to `logs.dlq`.
11. Health and stats endpoints expose operational state for demo and troubleshooting.

## 5. Scalability Plan

Scalable dimensions:

- Increase log generator clients to test higher traffic.
- Increase ingestion API replicas behind a load balancer.
- Increase Kafka partitions for higher parallelism.
- Increase worker replicas to consume more partitions concurrently.
- Tune worker bulk size, flush interval, and Elasticsearch refresh interval.
- Scale Elasticsearch from single-node local mode to a multi-node cluster.

Key design choices:

- Kafka protects the API from Elasticsearch slowdowns.
- Consumer groups allow horizontal worker scaling.
- Bulk indexing reduces Elasticsearch write overhead.
- Stateless API and worker services are easy to replicate.
- Configuration is environment-driven to support local and production-like deployments.

## 6. Reliability And Error Handling

Ingestion errors:

- Malformed JSON returns `400 Bad Request`.
- Oversized batches return `413 Payload Too Large`.
- Invalid log records return `422 Unprocessable Entity`.
- Kafka unavailable returns `503 Service Unavailable`.
- Rate limit exceeded returns `429 Too Many Requests`.

Processing errors:

- Parse failures go to `logs.dlq`.
- Temporary Elasticsearch failures are retried.
- Permanent Elasticsearch mapping failures go to `logs.dlq`.
- Worker commits Kafka offsets only after a record is stored or safely moved to DLQ.

Operational reliability:

- Use context timeouts for HTTP, Kafka, and Elasticsearch operations.
- Use graceful shutdown so services stop accepting new work before exiting.
- Use structured JSON logs for all services.
- Include correlation fields such as source, batch ID, and worker ID.

## 7. Monitoring Without Prometheus

Since Prometheus is not preferred, monitoring will be implemented with lightweight built-in and platform-level tools:

- `GET /healthz`: service process is alive
- `GET /readyz`: dependencies are reachable
- `GET /stats`: JSON counters and runtime metrics
- `docker stats`: CPU, memory, network, and container-level usage
- Kafka CLI tools: topic status, consumer groups, lag
- Elasticsearch APIs:
  - `GET /_cluster/health`
  - `GET /_cat/indices?v`
  - `GET /_cat/count/logs-*?v`
- Service logs in JSON format for ingestion errors, Kafka publish failures, worker retries, and DLQ events

Suggested `/stats` fields:

```json
{
  "service": "log-api",
  "uptime_seconds": 3600,
  "requests_total": 120000,
  "records_received_total": 5000000,
  "records_rejected_total": 120,
  "kafka_publish_errors_total": 2,
  "avg_batch_size": 250,
  "goroutines": 48,
  "memory_alloc_bytes": 73400320
}
```

## 8. Repository Structure Plan

```text
.
|-- cmd/
|   |-- log-api/
|   |   `-- main.go
|   |-- log-worker/
|   |   `-- main.go
|   `-- log-generator/
|       `-- main.go
|-- internal/
|   |-- config/
|   |-- ingestion/
|   |-- kafka/
|   |-- logline/
|   |-- metrics/
|   |-- storage/
|   `-- worker/
|-- deployments/
|   |-- docker-compose.yml
|   |-- elasticsearch/
|   |   `-- index-template.json
|   `-- kafka/
|-- scripts/
|   |-- run-demo.sh
|   |-- capture-before.sh
|   |-- capture-running.sh
|   |-- capture-after.sh
|   `-- check-system.sh
|-- docs/
|   |-- architecture.md
|   |-- operations.md
|   `-- test-report.md
|-- requirements/
|   `-- log-processing-system-plan.md
|-- go.mod
|-- go.sum
`-- README.md
```

## 9. Configuration Plan

Use environment variables with sensible local defaults:

```text
APP_PORT=8080
KAFKA_BROKERS=kafka:9092
KAFKA_LOG_TOPIC=logs.raw
KAFKA_RETRY_TOPIC=logs.retry
KAFKA_DLQ_TOPIC=logs.dlq
KAFKA_CONSUMER_GROUP=log-workers
ELASTICSEARCH_URL=http://elasticsearch:9200
MAX_BATCH_SIZE=1000
REQUEST_TIMEOUT=5s
WORKER_BULK_SIZE=1000
WORKER_FLUSH_INTERVAL=2s
WORKER_RETRY_MAX=5
```

## 10. Docker Compose Plan

Compose services:

- `log-api`: Go ingestion API
- `log-worker`: Go Kafka consumer and Elasticsearch writer
- `log-generator`: optional profile-based traffic generator
- `kafka`: Kafka broker, preferably KRaft mode to avoid ZooKeeper
- `elasticsearch`: single-node Elasticsearch
- `kibana`: optional for inspecting indexed logs, if allowed by project scope

Compose should include:

- health checks for Kafka and Elasticsearch
- named volumes for Elasticsearch data
- service dependency ordering
- environment variables for all Go services
- optional scaling commands for workers, for example:

```bash
docker compose up --scale log-worker=3
```

## 11. Implementation Milestones

### Week 1

Day 1:

- Finalize architecture and repository structure.
- Initialize Go module.
- Create Docker Compose with Kafka and Elasticsearch.
- Add Makefile or scripts for common commands.

Day 2:

- Implement log line parser and validation package.
- Add unit tests for valid and invalid log formats.
- Define Elasticsearch index template.

Day 3:

- Implement Log Processing API.
- Add batch validation, JSON request handling, health endpoints, and stats endpoint.
- Add Kafka producer integration.

Day 4:

- Implement worker Kafka consumer.
- Add parser integration and Elasticsearch bulk indexing.
- Add retry and DLQ handling.

Day 5:

- Implement log generator with TPS, client count, duration, batch size, and traffic mode options.
- Run first end-to-end test locally.
- Record early throughput and error observations.

### Week 2

Day 6:

- Improve backpressure handling in API.
- Add request limits, timeouts, and graceful shutdown.
- Tune Kafka producer and worker consumer settings.

Day 7:

- Add operational scripts for setup, demo runs, system checks, and stats collection.
- Add Docker stats capture scripts for report screenshots.

Day 8:

- Run performance tests at multiple TPS levels.
- Tune worker bulk size, flush interval, Kafka partitions, and Elasticsearch settings.
- Validate consumer lag under burst traffic.

Day 9:

- Write documentation:
  - installation guide
  - execution guide
  - architecture documentation
  - data processing flow
  - troubleshooting guide

Day 10:

- Produce final test report.
- Capture required screenshots:
  - CPU and RAM before running
  - CPU and RAM while running
  - CPU and RAM after completion
- Clean up README and verify all commands from a fresh checkout.

Buffer days:

- Use remaining time for unexpected integration issues, performance tuning, documentation polish, or demo rehearsal.

## 12. Test Strategy

Unit tests:

- Log parser accepts required format.
- Parser rejects missing fields, invalid timestamp, invalid IP, invalid status, and unsupported method.
- Config loader handles defaults and environment overrides.

Integration tests:

- API accepts valid batch and publishes to Kafka.
- API rejects invalid payloads.
- Worker consumes Kafka records and writes to Elasticsearch.
- Worker sends invalid records to DLQ.

Load tests:

- Run generator at increasing TPS: 1,000, 5,000, 10,000, and higher if hardware allows.
- Track API request latency, effective records per second, Kafka lag, Elasticsearch document count, CPU, and memory.
- Verify no data loss by comparing generated count, accepted count, indexed count, and DLQ count.

## 13. Demo And Report Evidence

Required screenshots:

- Before run:
  - `docker stats` or host system monitor showing baseline CPU and RAM.
- During run:
  - `docker stats` while generator is actively sending logs.
  - API `/stats` output.
  - worker `/stats` output.
  - Kafka consumer lag output.
- After run:
  - `docker stats` after traffic stops.
  - Elasticsearch document count.
  - generator summary showing total sent and effective TPS.

Suggested commands:

```bash
docker compose ps
docker stats
curl http://localhost:8080/healthz
curl http://localhost:8080/stats
curl http://localhost:8081/stats
curl http://localhost:9200/_cluster/health?pretty
curl 'http://localhost:9200/_cat/count/logs-*?v'
docker compose exec kafka kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group log-workers
```

## 14. Definition Of Done

The project is complete when:

- Log generator can produce configurable continuous traffic.
- API can receive concurrent batches and publish them to Kafka.
- Worker can consume, process, and index logs into Elasticsearch.
- Invalid records and downstream failures are handled predictably.
- Backpressure behavior is documented and observable.
- Health and stats endpoints exist for API and worker.
- Docker Compose can run the full system locally.
- README contains installation and execution instructions.
- Architecture and data flow documentation are complete.
- Final report includes execution results and required CPU/RAM screenshots.

