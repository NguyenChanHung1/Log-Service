package dashboard

import "time"

type Config struct {
	ElasticsearchURL string
	KafkaBrokers     string
	KafkaLogTopic    string
	KafkaConsumer    string
	LogAPIURL        string
	WorkerAPIURL     string
	StreamBuffer     int
}

type RuntimeStats struct {
	UptimeSeconds      int64   `json:"uptime_seconds"`
	Goroutines         int     `json:"goroutines"`
	MemoryAllocBytes   uint64  `json:"memory_alloc_bytes"`
	ProcessCPUSeconds  float64 `json:"process_cpu_seconds"`
	CPUPercentEstimate float64 `json:"cpu_percent_estimate"`
}

type ServiceStatus struct {
	Name    string `json:"name"`
	URL     string `json:"url,omitempty"`
	Status  string `json:"status"`
	Detail  string `json:"detail,omitempty"`
	Latency int64  `json:"latency_ms,omitempty"`
}

type Overview struct {
	Status         string          `json:"status"`
	GeneratedAt    time.Time       `json:"generated_at"`
	Runtime        RuntimeStats    `json:"runtime"`
	Services       []ServiceStatus `json:"services"`
	LogAPIStats    map[string]any  `json:"log_api_stats,omitempty"`
	Elasticsearch  map[string]any  `json:"elasticsearch"`
	Kafka          map[string]any  `json:"kafka"`
	RecentLogCount int64           `json:"recent_log_count"`
}

type MetricsPoint struct {
	TimeBucket time.Time `json:"time_bucket"`
	Count      int64     `json:"count"`
}

type MetricsResponse struct {
	From   time.Time      `json:"from"`
	To     time.Time      `json:"to"`
	IP     string         `json:"ip,omitempty"`
	Path   string         `json:"path,omitempty"`
	Points []MetricsPoint `json:"points"`
}

type LogRecord struct {
	Timestamp   time.Time `json:"@timestamp"`
	Source      string    `json:"source"`
	IP          string    `json:"ip"`
	Method      string    `json:"method"`
	Path        string    `json:"path"`
	Status      int       `json:"status"`
	Raw         string    `json:"raw"`
	ReceivedAt  time.Time `json:"received_at"`
	ProcessedAt time.Time `json:"processed_at,omitempty"`
	WorkerID    string    `json:"worker_id,omitempty"`
}

type LogsResponse struct {
	From  time.Time   `json:"from"`
	To    time.Time   `json:"to"`
	Limit int         `json:"limit"`
	Total int64       `json:"total"`
	Logs  []LogRecord `json:"logs"`
}
