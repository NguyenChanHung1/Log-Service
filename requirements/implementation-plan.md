# Log Processing System Implementation Plan

## 1. Purpose

This document converts the architecture plan into an actionable implementation plan for building the log processing system in roughly two weeks.

The target implementation uses Go, Kafka, Elasticsearch, a lightweight dashboard UI, and Docker Compose. Prometheus is excluded by design; service health and operational visibility will be provided through HTTP endpoints, Docker stats, Kafka CLI checks, Elasticsearch APIs, structured application logs, and the project dashboard.

## 2. Implementation Principles

- Build the smallest end-to-end pipeline first, then improve throughput and reliability.
- Keep the API and worker stateless so they can scale horizontally.
- Use Kafka as the durable buffer between ingestion and Elasticsearch.
- Treat data loss as unacceptable for accepted logs: acknowledge only after writing to Kafka or another durable fallback buffer.
- Use batched HTTP ingestion and Elasticsearch bulk indexing for performance.
- Add a dashboard UI for operational visibility: CPU/RAM, ingestion rates, Kafka lag, Elasticsearch status, searchable logs, and real-time logs.
- Keep configuration environment-driven.
- Add tests close to the logic that carries the most risk: parsing, validation, retry decisions, and storage flow.
- Document commands as they are built so the final report is easy to assemble.

## 3. Target Repository Layout

```text
.
|-- cmd/
|   |-- log-api/
|   |   `-- main.go
|   |-- dashboard-api/
|   |   `-- main.go
|   |-- log-worker/
|   |   `-- main.go
|   `-- log-generator/
|       `-- main.go
|-- web/
|   `-- dashboard/
|       |-- package.json
|       `-- src/
|-- internal/
|   |-- config/
|   |-- dashboard/
|   |-- ingestion/
|   |-- kafka/
|   |-- logline/
|   |-- metrics/
|   |-- spool/
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
|   |-- implementation-plan.md
|   `-- full-workflow-diagram.md
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
- `make build` builds all Go service binaries.
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

Goal: run Kafka, Elasticsearch, and the dashboard locally in a repeatable way.

Tasks:

- Create Docker Compose file using Kafka in KRaft mode.
- Add Elasticsearch single-node service.
- Add dashboard UI service and dashboard API service.
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
- Dashboard UI is reachable from the browser.

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
- Return `202 Accepted` after Kafka acknowledgement, or after writing to a durable local spool when Kafka is temporarily saturated or unavailable.
- Return clear error status codes:
  - `400` for malformed JSON
  - `413` for oversized request
  - `422` for invalid records
  - `503` only when both Kafka and the durable spool are unavailable
- Add graceful shutdown.
- Add an internal spool publisher that drains durable spooled records back into Kafka when capacity returns.

Suggested files:

- `cmd/log-api/main.go`
- `internal/ingestion/handler.go`
- `internal/ingestion/request.go`
- `internal/kafka/producer.go`
- `internal/spool/writer.go`
- `internal/spool/replayer.go`

Acceptance checks:

- Valid batch returns `202`.
- Invalid JSON returns `400`.
- Invalid log record returns `422`.
- Kafka publish failure writes accepted records to the durable spool and still returns `202`.
- API returns `503` only when no durable write path is available.
- API shuts down without dropping in-flight requests.

### WP5: API Health, Readiness, And Stats

Goal: provide machine-readable monitoring data without Prometheus.

Tasks:

- Add `GET /healthz` for process liveness.
- Add `GET /readyz` for Kafka readiness.
- Add `GET /stats` with JSON counters.
- Track request count, accepted records, rejected records, Kafka publish failures, spooled records, replayed records, average batch size, uptime, goroutine count, CPU estimate, and memory allocation.
- Add request dimensions needed by the dashboard:
  - IP
  - path
  - method
  - status
  - source
  - time bucket
- Emit structured JSON logs for request failures and Kafka errors.

Suggested files:

- `internal/metrics/counters.go`
- `internal/metrics/http.go`
- `internal/ingestion/middleware.go`

Acceptance checks:

- `/healthz` returns success while process is running.
- `/readyz` fails when Kafka cannot be reached.
- `/stats` returns useful JSON during load.
- `/stats` exposes enough data for dashboard CPU/RAM and request charts.

### WP6: Dashboard UI And Query API

Goal: provide a browser UI for monitoring resources, filtering requests, and viewing logs in real time.

Tasks:

- Implement dashboard API endpoints:
  - `GET /api/overview` for current service health, CPU/RAM, ingest rate, index rate, Kafka lag, and error counts
  - `GET /api/metrics?from=&to=&ip=&path=` for time-range charts
  - `GET /api/logs?from=&to=&ip=&path=&status=&limit=` for searchable stored logs
  - `GET /api/logs/stream` for real-time log streaming using Server-Sent Events
- Implement dashboard UI pages:
  - Overview dashboard with CPU/RAM cards and time-series charts
  - Request filters by IP, path, and time range
  - Kafka panel showing topic depth, consumer lag, spooled records, and replay rate
  - Elasticsearch panel showing cluster health, index count, failed writes, and retry backlog
  - Real-time logs page that streams newest logs as they are processed
- Store short-lived service metrics in memory for local demo and query Elasticsearch for historical log filtering.
- Use Docker stats or lightweight host/container sampling in the dashboard API for CPU/RAM in local Compose.
- Avoid Prometheus; do not add Prometheus server, exporters, or PromQL dependency.

Suggested files:

- `cmd/dashboard-api/main.go`
- `internal/dashboard/http.go`
- `internal/dashboard/metrics.go`
- `internal/dashboard/logs.go`
- `web/dashboard/package.json`
- `web/dashboard/src/App.tsx`
- `web/dashboard/src/pages/Overview.tsx`
- `web/dashboard/src/pages/RealtimeLogs.tsx`
- `web/dashboard/src/components/Filters.tsx`

Acceptance checks:

- Dashboard loads from Docker Compose.
- CPU and RAM are visible before, during, and after a run.
- Requests can be filtered by IP, path, and time range.
- Stored logs can be searched from the UI.
- Real-time logs page updates while the generator is running.

### WP7: Worker Consumer And Elasticsearch Writer

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
- Commit Kafka offsets only after indexing succeeds, the record is safely moved to DLQ, or the failed bulk is durably stored for replay.
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

### WP8: Worker Retry, Replay, And DLQ Handling

Goal: handle bad records and downstream failures predictably without dropping accepted data.

Tasks:

- Send parse failures to `logs.dlq`.
- Retry temporary Elasticsearch failures with exponential backoff.
- Pause or slow Kafka partition consumption when Elasticsearch is unhealthy so Kafka becomes the primary durable backlog.
- For prolonged Elasticsearch outages, write failed bulk payloads to a durable replay spool or Kafka retry topic instead of dropping input data.
- Replay failed bulk payloads after Elasticsearch recovers.
- Send only permanent document-level failures to `logs.dlq`, such as invalid mappings or poison records.
- Include failure reason and original payload in DLQ messages.
- Track worker stats:
  - consumed records
  - indexed records
  - failed records
  - retried records
  - DLQ records
  - Elasticsearch retry backlog
  - durable replay spool size
  - current batch size
- Add worker `GET /healthz`, `GET /readyz`, and `GET /stats`.

Suggested files:

- `internal/worker/retry.go`
- `internal/worker/dlq.go`
- `internal/worker/replay.go`
- `internal/kafka/producer.go`
- `internal/metrics/counters.go`
- `internal/spool/writer.go`
- `internal/spool/replayer.go`

Acceptance checks:

- Invalid raw records appear in DLQ.
- Temporary Elasticsearch outage triggers retries.
- Prolonged Elasticsearch outage stores failed batches in a durable replay path.
- When Elasticsearch recovers, replayed records are indexed.
- Worker stats show indexed, retried, and DLQ counts.

### WP9: Log Generator

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

### WP10: Backpressure, Durable Spooling, And Capacity Controls

Goal: keep the system stable when incoming traffic exceeds processing capacity without casually rejecting logs.

Rationale:

The log service is intended to support systems with lower load and capacity than the log platform itself. If Kafka is full, immediately returning `429` makes the logging system push failure back onto applications that may already be under pressure. A better design is layered degradation: absorb spikes in Kafka, then use a durable local spool, then slow ingestion in a controlled way, and reject only when there is no safe place left to persist data.

Tasks:

- Add API request timeout.
- Add maximum body size and maximum batch size.
- Detect high Kafka publish latency, broker errors, and topic size/lag warning thresholds.
- Add a bounded in-memory queue only as a short handoff buffer, not as the durable reliability layer.
- Add durable local disk spool with a configured max size and mounted Docker volume.
- Return `202 Accepted` when records are persisted to Kafka or durable spool.
- Run a spool replayer that publishes spooled records to Kafka when broker capacity returns.
- Add admission control based on durable capacity:
  - normal mode: write directly to Kafka
  - degraded mode: write to durable spool and expose warning in stats/dashboard
  - critical mode: accept only within reserved emergency spool capacity
  - exhausted mode: return `503 Service Unavailable` with `Retry-After` because no durable path remains
- Use `429` only for optional artificial test limits or abusive clients, not for ordinary Kafka saturation.
- Document the overload modes and their operational meaning.

Suggested files:

- `internal/ingestion/limits.go`
- `internal/ingestion/middleware.go`
- `internal/spool/writer.go`
- `internal/spool/replayer.go`
- `internal/spool/manifest.go`
- `docs/operations.md`

Acceptance checks:

- Oversized batches are rejected.
- API remains responsive under high concurrency.
- Kafka saturation switches API into durable spool mode.
- Spool backlog is observable in `/stats` and the dashboard.
- Spool drains automatically after Kafka recovers.
- `503` happens only when Kafka and durable spool capacity are both unavailable.
- Kafka buffering protects Elasticsearch from direct ingestion spikes.

### WP11: Scripts, Documentation, And Final Report

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
- Include screenshots from the dashboard UI for CPU/RAM, filtered request charts, and real-time logs.

Suggested files:

- `README.md`
- `docs/architecture.md`
- `docs/operations.md`
- `docs/test-report.md`
- `scripts/*.sh`

Acceptance checks:

- A fresh user can follow README commands.
- Demo script runs an end-to-end scenario.
- Report includes before, during, and after resource usage evidence from dashboard and Docker stats.

## 5. Recommended Build Sequence

1. Create project foundation and config loader.
2. Implement parser and unit tests.
3. Add Docker Compose infrastructure.
4. Implement API without Kafka using a temporary mock publisher.
5. Replace mock publisher with Kafka producer.
6. Implement worker with Elasticsearch bulk indexing.
7. Add retry, replay, and DLQ handling.
8. Implement durable spool and Kafka saturation behavior.
9. Implement generator.
10. Add dashboard API and UI.
11. Add monitoring endpoints, structured logs, and real-time log streaming.
12. Tune throughput, replay, and backpressure.
13. Write final documentation and report.

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

### Day 6: Error Handling And Replay

- Add API error responses.
- Add worker retry behavior.
- Add DLQ handling.
- Add durable replay behavior for Elasticsearch failures.
- Add graceful shutdown to API and worker.

Deliverable:

- Invalid records and temporary failures are handled intentionally.

### Day 7: Generator

- Implement generator flags and concurrent clients.
- Add steady traffic mode.
- Add summary output.

Deliverable:

- Generator can drive the API at configurable TPS.

### Day 8: Backpressure And Durable Spool

- Add API limits, durable spool, and overload modes.
- Tune Kafka producer settings.
- Tune worker batch size and flush interval.

Deliverable:

- System remains stable under Kafka or worker saturation without losing accepted logs.

### Day 9: Dashboard API And UI

- Implement dashboard API endpoints.
- Implement overview dashboard with CPU/RAM, request charts, Kafka lag, and Elasticsearch status.
- Implement filters by IP, path, and time range.
- Implement real-time logs page.

Deliverable:

- Dashboard shows resource usage, filtered request data, and live logs.

### Day 10: Integration And Replay Testing

- Add integration tests or repeatable scripts for end-to-end validation.
- Compare generated, accepted, indexed, spooled, replayed, and DLQ counts.
- Test Kafka saturation and Elasticsearch outage recovery.

Deliverable:

- Repeatable end-to-end and recovery verification exists.

### Day 11: Load Testing

- Run load tests at increasing TPS.
- Capture dashboard stats, API stats, worker stats, Kafka lag, Elasticsearch count, CPU, and RAM.
- Tune obvious bottlenecks.

Deliverable:

- Performance results are available for the report.

### Day 12: Documentation And Report Assets

- Write README, architecture documentation, operations guide, and troubleshooting guide.
- Run final demo scenario.
- Capture screenshots:
  - before run
  - while running
  - after run
- Collect generator summary and Elasticsearch document count.

Deliverable:

- Docs are usable and raw evidence exists for final report.

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
DASHBOARD_API_PORT=8082
DASHBOARD_UI_PORT=3000
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
SPOOL_DIR=/data/log-service-spool
SPOOL_MAX_BYTES=1073741824
SPOOL_REPLAY_INTERVAL=2s
REALTIME_STREAM_BUFFER=1000
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
| Backpressure | Fill or pause Kafka | API writes to durable spool and reports degraded mode |
| Spool replay | Restore Kafka after saturation | Spooled records replay and backlog returns to zero |
| Generator | Run fixed duration test | Summary prints effective TPS and failures |
| Dashboard UI | Open dashboard during load | CPU/RAM, request filters, Kafka lag, ES status, and live logs are visible |
| Resource report | Capture Docker stats and dashboard screenshots | CPU/RAM before, during, after are documented |
| Elasticsearch outage | Stop or block Elasticsearch writes temporarily | Worker retries or durably spools failed bulks, then replays after recovery |

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

4. Capture running CPU/RAM, dashboard screenshots, live logs, and service stats:

   ```bash
   scripts/capture-running.sh
   ```

5. Capture final CPU/RAM, dashboard state, spool backlog, replay counts, and Elasticsearch counts:

   ```bash
   scripts/capture-after.sh
   ```

6. Record final numbers:

   - generated records
   - accepted records
   - indexed records
   - spooled records
   - replayed records
   - DLQ records
   - failed requests
   - effective TPS
   - peak CPU/RAM
   - dashboard filter screenshots by IP, path, and time range
   - real-time logs screenshot

## 10. Risk Register

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Elasticsearch bulk indexing is slow | Kafka lag grows | Tune bulk size, flush interval, refresh interval, and worker replicas |
| Elasticsearch write outage | Accepted data could be stranded | Keep offsets uncommitted for short outages, then use durable retry spool or retry topic and replay after recovery |
| Kafka setup consumes too much time | Delays API/worker integration | Use a known Compose image and keep local replication factor at 1 |
| Generator cannot hit desired TPS locally | Weak demo evidence | Batch requests, increase clients gradually, document hardware limits |
| Data count mismatch | Report credibility issue | Track generated, accepted, indexed, failed, and DLQ counters from day one |
| Kafka queue becomes full | Client systems may be forced to handle logging failures | Use durable local spool, degraded mode, replay, and reserve `503` for exhausted durable capacity |
| Dashboard scope grows too large | Core ingestion work slips | Keep UI focused on overview, filters, and real-time logs; avoid heavy analytics features |
| Documentation left too late | Submission risk | Update README and operations notes as commands stabilize |

## 11. Final Acceptance Criteria

- The system runs locally using Docker Compose.
- The generator sends logs continuously at configurable TPS.
- The API receives concurrent batches and publishes valid records to Kafka.
- Kafka buffers records and supports worker scaling through partitions and consumer groups.
- If Kafka is saturated, accepted records are written to a durable spool and replayed later.
- The worker consumes records, parses them, and bulk indexes documents into Elasticsearch.
- Elasticsearch write failures are retried or durably replayed; accepted input data is not dropped.
- Invalid or permanently failing records are captured in DLQ.
- Health, readiness, and JSON stats endpoints exist for API, worker, and dashboard API.
- Dashboard UI displays CPU/RAM, request filters by IP/path/time range, Elasticsearch status, Kafka status, and real-time logs.
- Backpressure, durable spooling, and replay behavior are implemented and documented.
- Load test results demonstrate stable operation under the chosen test volume.
- Final documentation includes installation, execution, architecture, data flow, operations, and report evidence.

