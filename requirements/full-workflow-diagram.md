# Full Workflow Diagram

This diagram describes the complete flow from a user starting the log generator to logs being accepted, buffered, processed, stored, and observed.

```mermaid
flowchart LR
    User["User / Tester"] -->|runs command with TPS, clients, duration| Generator["Log Generator<br/>Go CLI"]

    Generator -->|creates log lines<br/>&lt;timestamp&gt; &lt;ip&gt; &lt;method&gt; &lt;path&gt; &lt;status&gt;| Batch["Batch Builder<br/>JSON payload"]
    Batch -->|HTTP POST /v1/logs| API["Log Processing API<br/>Go service"]

    API --> Validate["Validate Request<br/>body size, batch size,<br/>source, log format"]

    Validate -->|invalid JSON| BadRequest["400 Bad Request"]
    Validate -->|batch too large| TooLarge["413 Payload Too Large"]
    Validate -->|invalid log records| Invalid["422 Unprocessable Entity"]
    Validate -->|valid records| Producer["Kafka Producer"]
    Producer -->|acknowledged write| RawTopic[("Kafka Topic<br/>logs.raw")]
    Producer -->|Kafka saturated or temporarily unavailable| Spool[("Durable Local Spool<br/>mounted volume")]
    Spool -->|replay when Kafka recovers| RawTopic
    Producer -->|Kafka and spool unavailable| Unavailable["503 Service Unavailable<br/>Retry-After"]

    API -->|202 Accepted after Kafka or spool durability| Generator

    RawTopic -->|consume by partition| WorkerGroup["Log Worker Consumer Group<br/>1..N replicas"]
    WorkerGroup --> Parse["Parse And Enrich<br/>timestamp, ip, method,<br/>path, status, metadata"]

    Parse -->|parse failure| DLQProducer["DLQ Producer"]
    DLQProducer --> DLQ[("Kafka Topic<br/>logs.dlq")]

    Parse -->|valid document| BulkBuffer["Bulk Buffer<br/>size/time flush"]
    BulkBuffer -->|bulk index request| Elasticsearch[("Elasticsearch<br/>logs-YYYY.MM.DD")]

    Elasticsearch -->|success| Commit["Commit Kafka Offset"]
    Elasticsearch -->|temporary failure| Retry["Retry With Backoff"]
    Retry -->|retry succeeds| Commit
    Retry -->|prolonged ES outage| ReplaySpool[("Durable Replay Spool<br/>failed bulk payloads")]
    ReplaySpool -->|replay after ES recovers| BulkBuffer
    Retry -->|permanent document failure| DLQProducer

    Commit --> StatsWorker["Worker Stats<br/>consumed, indexed,<br/>retried, DLQ"]

    API --> StatsAPI["API Stats<br/>requests, accepted,<br/>rejected, Kafka errors"]
    API --> HealthAPI["/healthz<br/>/readyz<br/>/stats"]
    WorkerGroup --> HealthWorker["/healthz<br/>/readyz<br/>/stats"]
    Elasticsearch --> ESHealth["Elasticsearch APIs<br/>/_cluster/health<br/>/_cat/count/logs-*"]
    RawTopic --> KafkaLag["Kafka CLI<br/>consumer lag"]
    Spool --> SpoolStats["Spool Stats<br/>backlog and replay rate"]
    ReplaySpool --> ReplayStats["Replay Stats<br/>failed bulk backlog"]

    Dashboard["Dashboard UI<br/>Overview and Real-Time Logs"] --> DashboardAPI["Dashboard API"]
    DashboardAPI -->|CPU/RAM| DockerStats["Docker Stats<br/>CPU/RAM before,<br/>during, after"]
    DashboardAPI -->|request charts| StatsAPI
    DashboardAPI -->|worker charts| StatsWorker
    DashboardAPI -->|filtered logs by ip, path, time| Elasticsearch
    DashboardAPI -->|SSE live stream| WorkerGroup
    DashboardAPI --> KafkaLag
    DashboardAPI --> SpoolStats
    DashboardAPI --> ReplayStats

    StatsAPI --> Report["Execution Report<br/>screenshots and metrics"]
    StatsWorker --> Report
    HealthAPI --> Report
    HealthWorker --> Report
    ESHealth --> Report
    KafkaLag --> Report
    Generator -->|summary: generated,<br/>accepted, failed, TPS| Report
    DockerStats --> Report
    Dashboard -->|screenshots: CPU/RAM,<br/>filters, live logs| Report
```

## Sequence View

```mermaid
sequenceDiagram
    autonumber
    actor User as User / Tester
    participant Gen as Log Generator
    participant API as Log Processing API
    participant Kafka as Kafka logs.raw
    participant Spool as Durable Spool
    participant Worker as Log Worker
    participant ES as Elasticsearch
    participant DLQ as Kafka logs.dlq
    participant Dash as Dashboard UI
    participant Ops as Monitoring / Report

    User->>Gen: Start with target TPS, clients, duration, batch size
    Gen->>Gen: Generate required log format
    Gen->>API: POST /v1/logs with JSON batch
    API->>API: Validate body, batch size, source, log format

    alt Invalid request
        API-->>Gen: 400 / 413 / 422
    else Valid request and Kafka has capacity
        API->>Kafka: Produce records
        Kafka-->>API: Acknowledge write
        API-->>Gen: 202 Accepted
    else Kafka saturated or temporarily unavailable
        API->>Spool: Persist records to durable local spool
        Spool-->>API: Durable write confirmed
        API-->>Gen: 202 Accepted in degraded mode
    else Kafka and spool unavailable
        API-->>Gen: 503 Service Unavailable with Retry-After
    end

    Spool->>Kafka: Replay spooled records when broker capacity returns
    Worker->>Kafka: Consume records by consumer group
    Worker->>Worker: Parse and enrich

    alt Parse failure
        Worker->>DLQ: Publish original record with failure reason
        Worker->>Kafka: Commit offset
    else Valid record
        Worker->>ES: Bulk index document
        alt Index success
            ES-->>Worker: Success
            Worker->>Kafka: Commit offset
        else Temporary indexing failure
            Worker->>Worker: Retry with backoff
            Worker->>ES: Bulk index retry
        else Prolonged Elasticsearch outage
            Worker->>Spool: Persist failed bulk for replay
            Worker->>Kafka: Commit offset after durable replay write
        else Permanent document failure
            Worker->>DLQ: Publish original record with failure reason
            Worker->>Kafka: Commit offset
        end
    end

    Dash->>API: GET /stats for request charts
    Dash->>Worker: GET /stats for processing charts
    Dash->>ES: Query logs by IP, path, and time range
    Dash->>Worker: Subscribe to /api/logs/stream for real-time logs
    Ops->>API: GET /healthz /readyz /stats
    Ops->>Worker: GET /healthz /readyz /stats
    Ops->>Kafka: Check topic status and consumer lag
    Ops->>ES: Check cluster health and document counts
    Ops->>Ops: Capture Docker CPU/RAM before, during, after
```

