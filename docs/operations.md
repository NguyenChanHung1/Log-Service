# Operations

## Start

```bash
make build
docker compose -f deployments/docker-compose.yml --profile app up --build
```

Open:

- Dashboard UI: `http://localhost:3000`
- Log API stats: `http://localhost:8080/stats`
- Worker stats: `http://localhost:8081/stats`
- Dashboard API: `http://localhost:8082/api/overview`

## Check Health

```bash
make check-system
curl http://localhost:8080/readyz
curl http://localhost:8081/readyz
curl http://localhost:8082/api/overview
```

## Run A Demo Load

```bash
make build
./scripts/run-demo.sh
```

## Capture Report Evidence

Run before, during, and after the demo:

```bash
./scripts/capture-before.sh
./scripts/capture-running.sh
./scripts/capture-after.sh
```

The scripts write text evidence to `docs/evidence/`. Capture screenshots manually from:

- Docker Desktop or system monitor for CPU/RAM.
- `http://localhost:3000` for dashboard CPU/RAM cards.
- Dashboard Logs page for filtered request charts.
- Dashboard Real-time page while the generator is running.

## Troubleshooting

- Kafka readiness fails: check `docker compose -f deployments/docker-compose.yml ps` and Kafka logs.
- Elasticsearch readiness fails: check `curl http://localhost:9200/_cluster/health?pretty`.
- API returns `503`: inspect `curl http://localhost:8080/stats`; if `overload_mode` is `critical` or `exhausted`, wait for spool replay or increase `SPOOL_MAX_BYTES`.
- Stored logs are empty: confirm worker stats `indexed_records` increases and Elasticsearch has `logs-*` indexes.
- DLQ records exist: inspect the `logs.dlq` topic; poison records include failure reason metadata.

## Shutdown

```bash
docker compose -f deployments/docker-compose.yml --profile app down
```
