# Test Report

## Environment

- Date:
- Machine:
- CPU:
- RAM:
- Docker version:
- Go version:

## Commands

```bash
go test ./...
make build
docker compose -f deployments/docker-compose.yml --profile app up --build
./scripts/capture-before.sh
./bin/log-generator --target http://localhost:8080/v1/logs --clients 4 --tps 500 --duration 30s --batch-size 100 --mode steady
./scripts/capture-running.sh
./scripts/capture-after.sh
docker compose -f deployments/docker-compose.yml --profile app down
```

## Results

| Check | Expected | Actual |
| --- | --- | --- |
| API health | `200 OK` | |
| Worker health | `200 OK` | |
| Dashboard API health | `200 OK` | |
| Generator failed requests | `0` or explained | |
| Elasticsearch document count | matches accepted records for small run | |
| API overload mode | normal/degraded/critical/exhausted as observed | |

## Screenshots

- CPU/RAM before run:
- CPU/RAM during run:
- CPU/RAM after run:
- Dashboard overview:
- Filtered logs page:
- Real-time logs page:

## Notes

Add observations about throughput, latency, spool usage, DLQ count, and any bottlenecks.
