# Architecture

## Components

- Log Generator: creates synthetic log records at configurable TPS and sends HTTP batches.
- Log API: validates records, publishes to Kafka, and falls back to durable disk spool when Kafka is unavailable or saturated.
- Kafka: durable buffer for `logs.raw`, `logs.retry`, and `logs.dlq`.
- Log Worker: consumes `logs.raw`, enriches documents, bulk-indexes to Elasticsearch, and safely handles retries/DLQ.
- Elasticsearch: stores searchable `logs-*` daily indexes.
- Dashboard API/UI: exposes service health, resource stats, filters, stored logs, and real-time log stream.

## Data Flow

1. Generator sends `POST /v1/logs` batches.
2. Log API validates body size, batch size, and each log line.
3. Valid records are published to Kafka.
4. If Kafka publish fails, accepted records are written to durable local spool.
5. API spool replayer republishes spooled records after Kafka recovers.
6. Worker consumes Kafka records and validates the raw line again.
7. Valid documents are bulk-indexed into daily Elasticsearch indexes.
8. Poison records and permanent document failures are written to `logs.dlq`.
9. Temporary Elasticsearch failures retry, then persist to worker replay spool.
10. Dashboard reads API/worker stats and Elasticsearch search results.

## Reliability Rules

- Accepted input is not dropped.
- API returns `202` only after Kafka or durable spool persistence.
- API returns `503` with `Retry-After` only when no safe ingestion capacity is available.
- Kafka offsets are committed only after indexing, DLQ, or durable worker replay storage.
- Elasticsearch outages produce backlog instead of data loss.

## Overload Modes

- `normal`: Kafka is accepting writes and spool is empty.
- `degraded`: Kafka publish failed or spool backlog exists.
- `critical`: spool usage is at least 80 percent of capacity.
- `exhausted`: spool capacity is full; API returns `503`.
