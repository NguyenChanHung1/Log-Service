# Log Processing System Implementation Plan

## 1. Purpose

This document converts the architecture plan into an actionable implementation plan for building the log processing system in roughly two weeks.

The target implementation uses Go, Kafka, Elasticsearch, and Docker Compose. Prometheus is excluded by design; service health and operational visibility will be provided through HTTP endpoints, Docker stats, Kafka CLI checks, Elasticsearch APIs, and structured application logs.

## 2. Implementation Principles

- Build the smallest end-to-end pipeline first, then improve throughput and reliability.
- Keep the API and worker stateless so they can scale horizontally.
- Use Kafka as the durable buffer between ingestion and Elasticsearch.
- Use batched HTTP ingestion and Elasticsearch bulk indexing for performance.
- Keep configuration environment-driven.
- Add tests close to the logic that carries the most risk: parsing, validation, retry decisions, and storage flow.
- Document commands as they are built so the final report is easy to assemble.

## 3. Target Repository Layout

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
|-- docs/
|   |-- architecture.md
|   |-- operations.md
|   `-- test-report.md
|-- requirements/
|   |-- log-processing-system-plan.md
|   `-- implementation-plan.md
|-- scripts/
|   |-- check-system.sh
|   |-- run-demo.sh
|   |-- capture-before.sh
|   |-- capture-running.sh
|   `-- capture-after.sh
|-- Makefile
|-- Dockerfile
|-- go.mod
|-- go.sum
`-- README.md
```

## 4. Work Packages

### WP1: Project Foundation

Goal: create the basic Go project, Docker build path, and developer commands.

Tasks:

- Initialize Go module.
- Add base packages under `internal/`.
- Add `Makefile` targets for build, test, compose up, compose down, and demo.
- Add a multi-stage `Dockerfile` for Go services.
- Add `.env.example` with local defaults.

Suggested files:

- `go.mod`
- `Makefile`
- `Dockerfile`
- `.env.example`
- `internal/config/config.go`

Acceptance checks:

- `go test ./...` runs successfully.
- `make build` builds all three binaries.
- Configuration loads defaults when environment variables are missing.

### WP2: Log Line Parser And Shared Types

Goal: define the core log data model and validation behavior.

Tasks:

- Implement parser for:

  ```text
  <timestamp> <ip> <method> <path> <status>
  ```

- Validate timestamp as RFC3339.
- Validate IP using Go standard library parsing.
- Validate method against common HTTP methods.
- Validate status as an integer from 100 to 599.
- Preserve the raw log line for traceability.
- Add unit tests for valid and invalid examples.

Suggested files:

- `internal/logline/parser.go`
- `internal/logline/types.go`
- `internal/logline/parser_test.go`

Acceptance checks:

- Valid examples parse into structured records.
- Missing fields, invalid IPs, invalid timestamps, bad status codes, and unsupported methods fail with clear errors.

### WP3: Local Infrastructure With Docker Compose

Goal: run Kafka and Elasticsearch locally in a repeatable way.

Tasks:

- Create Docker Compose file using Kafka in KRaft mode.
- Add Elasticsearch single-node service.
- Add optional Kibana service if useful for demo inspection.
- Create Kafka topics on startup:
  - `logs.raw`
  - `logs.retry`
  - `logs.dlq`
- Add Elasticsearch index template.
- Add health checks for Kafka and Elasticsearch.

Suggested files:

- `deployments/docker-compose.yml`
- `deployments/elasticsearch/index-template.json`
- `deployments/kafka/create-topics.sh`

Acceptance checks:

- `docker compose -f deployments/docker-compose.yml up` starts infrastructure.
- Kafka topics exist.
- Elasticsearch health endpoint returns a healthy local cluster state.
- Index template is installed or installable by script.

### WP4: Log Processing API

Goal: receive HTTP batches and publish accepted records to Kafka.

Tasks:

- Implement `POST /v1/logs`.
- Accept payload:

  ```json
  {
    "source": "client-001",
    "records": [
      "2026-07-07T09:00:01Z 10.10.1.5 GET /login 200"
    ]
  }
  ```

- Enforce maximum request body size.
- Enforce maximum batch size.
- Validate each record before publish.
- Publish accepted records to Kafka topic `logs.raw`.
- Return `202 Accepted` after Kafka acknowledgement.
- Return clear error status codes:
  - `400` for malformed JSON
  - `413` for oversized request
  - `422` for invalid records
  - `429` for rate limit or overload
  - `503` when Kafka is unavailable
- Add graceful shutdown.

Suggested files:

- `cmd/log-api/main.go`
- `internal/ingestion/handler.go`
- `internal/ingestion/request.go`
- `internal/kafka/producer.go`

Acceptance checks:

- Valid batch returns `202`.
- Invalid JSON returns `400`.
- Invalid log record returns `422`.
- Kafka publish failure returns `503`.
- API shuts down without dropping in-flight requests.

### WP5: API Health, Readiness, And Stats

Goal: provide monitoring without Prometheus.

Tasks:

- Add `GET /healthz` for process liveness.
- Add `GET /readyz` for Kafka readiness.
- Add `GET /stats` with JSON counters.
- Track request count, accepted records, rejected records, Kafka publish failures, average batch size, uptime, goroutine count, and memory allocation.
- Emit structured JSON logs for request failures and Kafka errors.

Suggested files:

- `internal/metrics/counters.go`
- `internal/metrics/http.go`
- `internal/ingestion/middleware.go`

Acceptance checks:

- `/healthz` returns success while process is running.
- `/readyz` fails when Kafka cannot be reached.
- `/stats` returns useful JSON during load.

### WP6: Worker Consumer And Elasticsearch Writer

Goal: consume logs from Kafka, parse them, and index them into Elasticsearch.

Tasks:

- Implement Kafka consumer group for `logs.raw`.
- Parse raw records into structured documents.
- Add metadata:
  - source
  - received timestamp
  - processed timestamp
  - worker ID
- Implement Elasticsearch bulk writer.
- Use daily index names, for example `logs-2026.07.07`.
- Commit Kafka offsets only after indexing succeeds.
- Add graceful shutdown and flush remaining batches before exit.

Suggested files:

- `cmd/log-worker/main.go`
- `internal/worker/consumer.go`
- `internal/worker/processor.go`
- `internal/storage/elasticsearch.go`
- `internal/storage/bulk.go`

Acceptance checks:

- Worker consumes records from Kafka.
- Documents appear in Elasticsearch.
- Elasticsearch document count matches accepted records during small test runs.
- Worker can be scaled with multiple replicas.

### WP7: Worker Retry And DLQ Handling

Goal: handle bad records and downstream failures predictably.

Tasks:

- Send parse failures to `logs.dlq`.
- Retry temporary Elasticsearch failures with exponential backoff.
- Send permanent Elasticsearch failures to `logs.dlq`.
- Include failure reason and original payload in DLQ messages.
- Track worker stats:
  - consumed records
  - indexed records
  - failed records
  - retried records
  - DLQ records
  - current batch size
- Add worker `GET /healthz`, `GET /readyz`, and `GET /stats`.

Suggested files:

- `internal/worker/retry.go`
- `internal/worker/dlq.go`
- `internal/kafka/producer.go`
- `internal/metrics/counters.go`

Acceptance checks:

- Invalid raw records appear in DLQ.
- Temporary Elasticsearch outage triggers retries.
- Worker stats show indexed, retried, and DLQ counts.

### WP8: Log Generator

Goal: simulate multiple log sources with configurable throughput.

Tasks:

- Implement command flags:
  - `--target`
  - `--clients`
  - `--tps`
  - `--duration`
  - `--batch-size`
  - `--mode steady|burst`
- Generate realistic IPs, methods, paths, and status codes.
- Send JSON batches to the API.
- Apply per-client pacing to approximate target TPS.
- Track and print:
  - generated records
  - sent records
  - accepted records
  - failed requests
  - retries
  - average latency
  - p95 latency
  - effective TPS

Suggested files:

- `cmd/log-generator/main.go`
- `internal/generator/generator.go`
- `internal/generator/client.go`
- `internal/generator/stats.go`

Acceptance checks:

- Generator can run for a fixed duration.
- Multiple clients send concurrently.
- Effective TPS is close to configured TPS under normal local load.
- Summary output is suitable for the final report.

### WP9: Backpressure And Capacity Controls

Goal: keep the system stable when incoming traffic exceeds processing capacity.

Tasks:

- Add API request timeout.
- Add maximum body size and maximum batch size.
- Add simple in-process rate limiter or concurrency limiter.
- Detect high Kafka publish latency.
- Return `429` for controlled overload.
- Return `503` for dependency unavailability.
- Document expected behavior under overload.

Suggested files:

- `internal/ingestion/limits.go`
- `internal/ingestion/middleware.go`
- `docs/operations.md`

Acceptance checks:

- Oversized batches are rejected.
- API remains responsive under high concurrency.
- Overload responses are observable in `/stats`.
- Kafka buffering protects Elasticsearch from direct ingestion spikes.

### WP10: Scripts, Documentation, And Final Report

Goal: make the project easy to run, verify, and present.

Tasks:

- Write README installation and execution guide.
- Write architecture documentation.
- Write operations and troubleshooting guide.
- Add scripts:
  - `scripts/check-system.sh`
  - `scripts/run-demo.sh`
  - `scripts/capture-before.sh`
  - `scripts/capture-running.sh`
  - `scripts/capture-after.sh`
- Prepare `docs/test-report.md`.
- Include commands for collecting CPU/RAM screenshots.

Suggested files:

- `README.md`
- `docs/architecture.md`
- `docs/operations.md`
- `docs/test-report.md`
- `scripts/*.sh`

Acceptance checks:

- A fresh user can follow README commands.
- Demo script runs an end-to-end scenario.
- Report includes before, during, and after resource usage evidence.

## 5. Recommended Build Sequence

1. Create project foundation and config loader.
2. Implement parser and unit tests.
3. Add Docker Compose infrastructure.
4. Implement API without Kafka using a temporary mock publisher.
5. Replace mock publisher with Kafka producer.
6. Implement worker with Elasticsearch bulk indexing.
7. Add retry and DLQ handling.
8. Implement generator.
9. Add monitoring endpoints and structured logs.
10. Tune throughput and backpressure.
11. Write final documentation and report.

This sequence keeps an end-to-end path visible early while avoiding a large integration cliff near the end.

## 6. Two-Week Execution Schedule

### Day 1: Foundation

- Initialize Go module.
- Add repository structure.
- Add Dockerfile, Makefile, and `.env.example`.
- Implement config loader.

Deliverable:

- Project builds with empty service entry points.

### Day 2: Parser

- Implement log parser and shared log types.
- Add parser unit tests.
- Add initial structured logging helper if needed.

Deliverable:

- Parser has strong test coverage for valid and invalid log lines.

### Day 3: Infrastructure

- Add Kafka and Elasticsearch to Docker Compose.
- Create Kafka topic setup.
- Add Elasticsearch index template.
- Add basic system check script.

Deliverable:

- Local infrastructure can start and pass basic checks.

### Day 4: API

- Implement API routes.
- Add request validation.
- Add Kafka producer.
- Add health, readiness, and stats endpoints.

Deliverable:

- API accepts valid batches and publishes to Kafka.

### Day 5: Worker

- Implement Kafka consumer group.
- Implement Elasticsearch bulk writer.
- Add worker health, readiness, and stats endpoints.

Deliverable:

- Logs flow from API to Kafka to Elasticsearch.

### Day 6: Error Handling

- Add API error responses.
- Add worker retry behavior.
- Add DLQ handling.
- Add graceful shutdown to API and worker.

Deliverable:

- Invalid records and temporary failures are handled intentionally.

### Day 7: Generator

- Implement generator flags and concurrent clients.
- Add steady traffic mode.
- Add summary output.

Deliverable:

- Generator can drive the API at configurable TPS.

### Day 8: Backpressure

- Add API limits and overload behavior.
- Tune Kafka producer settings.
- Tune worker batch size and flush interval.

Deliverable:

- System remains stable under traffic above worker capacity.

### Day 9: Integration Testing

- Add integration tests or repeatable scripts for end-to-end validation.
- Compare generated, accepted, indexed, and DLQ counts.
- Fix data loss or duplicate indexing issues found during testing.

Deliverable:

- Repeatable end-to-end verification exists.

### Day 10: Load Testing

- Run load tests at increasing TPS.
- Capture API stats, worker stats, Kafka lag, Elasticsearch count, CPU, and RAM.
- Tune obvious bottlenecks.

Deliverable:

- Performance results are available for the report.

### Day 11: Documentation

- Write README.
- Write architecture documentation.
- Write operations and troubleshooting guide.

Deliverable:

- Another developer can run the project from docs.

### Day 12: Report Assets

- Run final demo scenario.
- Capture screenshots:
  - before run
  - while running
  - after run
- Collect generator summary and Elasticsearch document count.

Deliverable:

- Raw evidence exists for final report.

### Day 13: Final Report

- Write `docs/test-report.md`.
- Include architecture summary, test setup, commands, results, screenshots, and limitations.
- Validate every command in README and report.

Deliverable:

- Final report is complete.

### Day 14: Buffer And Polish

- Fix final bugs.
- Improve docs clarity.
- Re-run a clean demo.
- Prepare final submission.

Deliverable:

- Repository is ready to submit.

## 7. Configuration Checklist

Required environment variables:

```text
APP_PORT=8080
WORKER_PORT=8081
KAFKA_BROKERS=kafka:9092
KAFKA_LOG_TOPIC=logs.raw
KAFKA_RETRY_TOPIC=logs.retry
KAFKA_DLQ_TOPIC=logs.dlq
KAFKA_CONSUMER_GROUP=log-workers
ELASTICSEARCH_URL=http://elasticsearch:9200
MAX_BATCH_SIZE=1000
MAX_BODY_BYTES=1048576
REQUEST_TIMEOUT=5s
WORKER_BULK_SIZE=1000
WORKER_FLUSH_INTERVAL=2s
WORKER_RETRY_MAX=5
```

Generator flags:

```text
--target http://localhost:8080/v1/logs
--clients 20
--tps 5000
--duration 10m
--batch-size 250
--mode steady
```

## 8. Verification Matrix

| Area | Verification | Expected Result |
| --- | --- | --- |
| Parser | `go test ./internal/logline` | Valid lines pass, invalid lines fail |
| API health | `curl localhost:8080/healthz` | HTTP 200 |
| API readiness | `curl localhost:8080/readyz` | HTTP 200 when Kafka is reachable |
| API ingestion | Send valid batch | HTTP 202 |
| API validation | Send invalid log line | HTTP 422 |
| Kafka | Describe `logs.raw` topic | Topic exists with configured partitions |
| Worker | Run worker with API traffic | Kafka lag decreases |
| Elasticsearch | Count `logs-*` documents | Count increases after ingestion |
| DLQ | Inject invalid Kafka record | Record appears in `logs.dlq` |
| Backpressure | Exceed configured capacity | API returns controlled `429` or `503` |
| Generator | Run fixed duration test | Summary prints effective TPS and failures |
| Resource report | Capture Docker stats | CPU/RAM before, during, after are documented |

## 9. Demo Scenario

Recommended final demo:

1. Start infrastructure and services:

   ```bash
   docker compose -f deployments/docker-compose.yml up --build
   ```

2. Capture baseline CPU/RAM:

   ```bash
   scripts/capture-before.sh
   ```

3. Run generator:

   ```bash
   go run ./cmd/log-generator \
     --target http://localhost:8080/v1/logs \
     --clients 20 \
     --tps 5000 \
     --duration 5m \
     --batch-size 250
   ```

4. Capture running CPU/RAM and service stats:

   ```bash
   scripts/capture-running.sh
   ```

5. Capture final CPU/RAM and Elasticsearch counts:

   ```bash
   scripts/capture-after.sh
   ```

6. Record final numbers:

   - generated records
   - accepted records
   - indexed records
   - DLQ records
   - failed requests
   - effective TPS
   - peak CPU/RAM

## 10. Risk Register

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Elasticsearch bulk indexing is slow | Kafka lag grows | Tune bulk size, flush interval, refresh interval, and worker replicas |
| Kafka setup consumes too much time | Delays API/worker integration | Use a known Compose image and keep local replication factor at 1 |
| Generator cannot hit desired TPS locally | Weak demo evidence | Batch requests, increase clients gradually, document hardware limits |
| Data count mismatch | Report credibility issue | Track generated, accepted, indexed, failed, and DLQ counters from day one |
| Overload behavior is unclear | System appears unstable under stress | Define explicit `429` and `503` behavior and expose counters |
| Documentation left too late | Submission risk | Update README and operations notes as commands stabilize |

## 11. Final Acceptance Criteria

- The system runs locally using Docker Compose.
- The generator sends logs continuously at configurable TPS.
- The API receives concurrent batches and publishes valid records to Kafka.
- Kafka buffers records and supports worker scaling through partitions and consumer groups.
- The worker consumes records, parses them, and bulk indexes documents into Elasticsearch.
- Invalid or permanently failing records are captured in DLQ.
- Health, readiness, and JSON stats endpoints exist for API and worker.
- Backpressure behavior is implemented and documented.
- Load test results demonstrate stable operation under the chosen test volume.
- Final documentation includes installation, execution, architecture, data flow, operations, and report evidence.

