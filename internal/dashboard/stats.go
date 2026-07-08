package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strings"
	"syscall"
	"time"

	segmentio "github.com/segmentio/kafka-go"
)

type Collector struct {
	cfg        Config
	started    time.Time
	httpClient *http.Client
	elastic    *elasticClient
}

func NewCollector(cfg Config) *Collector {
	return &Collector{
		cfg:        cfg,
		started:    time.Now().UTC(),
		httpClient: &http.Client{Timeout: 3 * time.Second},
		elastic:    newElasticClient(cfg.ElasticsearchURL),
	}
}

func (c *Collector) Overview(ctx context.Context) Overview {
	now := time.Now().UTC()
	from := now.Add(-15 * time.Minute)
	logStats := c.fetchJSON(ctx, strings.TrimRight(c.cfg.LogAPIURL, "/")+"/stats")
	services := []ServiceStatus{
		c.service(ctx, "log-api", strings.TrimRight(c.cfg.LogAPIURL, "/")+"/healthz"),
		c.service(ctx, "log-api-readiness", strings.TrimRight(c.cfg.LogAPIURL, "/")+"/readyz"),
		c.service(ctx, "log-worker", strings.TrimRight(c.cfg.WorkerAPIURL, "/")+"/healthz"),
		c.kafkaStatus(ctx),
	}

	return Overview{
		Status:         overviewStatus(services),
		GeneratedAt:    now,
		Runtime:        c.runtimeStats(),
		Services:       services,
		LogAPIStats:    logStats,
		Elasticsearch:  c.elastic.health(ctx),
		Kafka:          c.kafkaOverview(ctx),
		RecentLogCount: c.elastic.count(ctx, from, now),
	}
}

func (c *Collector) Metrics(ctx context.Context, from time.Time, to time.Time, ip string, path string) (MetricsResponse, error) {
	points, err := c.elastic.metrics(ctx, from, to, ip, path)
	if err != nil {
		return MetricsResponse{}, err
	}
	return MetricsResponse{From: from, To: to, IP: ip, Path: path, Points: points}, nil
}

func (c *Collector) Logs(ctx context.Context, from time.Time, to time.Time, ip string, path string, status string, limit int) (LogsResponse, error) {
	return c.elastic.logs(ctx, from, to, ip, path, status, limit)
}

func (c *Collector) runtimeStats() RuntimeStats {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	uptime := int64(time.Since(c.started).Seconds())
	cpuSeconds := processCPUSeconds()
	cpuPercent := 0.0
	if uptime > 0 {
		cpuPercent = cpuSeconds / float64(uptime) * 100
	}

	return RuntimeStats{
		UptimeSeconds:      uptime,
		Goroutines:         runtime.NumGoroutine(),
		MemoryAllocBytes:   mem.Alloc,
		ProcessCPUSeconds:  cpuSeconds,
		CPUPercentEstimate: cpuPercent,
	}
}

func (c *Collector) service(ctx context.Context, name string, url string) ServiceStatus {
	if url == "" {
		return ServiceStatus{Name: name, Status: "unknown", Detail: "url is not configured"}
	}

	started := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return ServiceStatus{Name: name, URL: url, Status: "unavailable", Detail: err.Error()}
	}
	res, err := c.httpClient.Do(req)
	if err != nil {
		return ServiceStatus{Name: name, URL: url, Status: "unavailable", Detail: err.Error()}
	}
	defer res.Body.Close()

	status := "ok"
	if res.StatusCode >= 400 {
		status = "degraded"
	}
	return ServiceStatus{Name: name, URL: url, Status: status, Detail: res.Status, Latency: time.Since(started).Milliseconds()}
}

func (c *Collector) fetchJSON(ctx context.Context, url string) map[string]any {
	if url == "" {
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return map[string]any{"status": "unavailable", "error": err.Error()}
	}
	res, err := c.httpClient.Do(req)
	if err != nil {
		return map[string]any{"status": "unavailable", "error": err.Error()}
	}
	defer res.Body.Close()
	if res.StatusCode >= 400 {
		return map[string]any{"status": "unavailable", "error": res.Status}
	}

	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		return map[string]any{"status": "unavailable", "error": err.Error()}
	}
	return out
}

func (c *Collector) kafkaStatus(ctx context.Context) ServiceStatus {
	brokers := splitBrokers(c.cfg.KafkaBrokers)
	if len(brokers) == 0 {
		return ServiceStatus{Name: "kafka", Status: "unavailable", Detail: "no brokers configured"}
	}

	started := time.Now()
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return ServiceStatus{Name: "kafka", URL: brokers[0], Status: "unavailable", Detail: err.Error()}
	}
	_ = conn.Close()
	return ServiceStatus{Name: "kafka", URL: brokers[0], Status: "ok", Latency: time.Since(started).Milliseconds()}
}

func (c *Collector) kafkaOverview(ctx context.Context) map[string]any {
	brokers := splitBrokers(c.cfg.KafkaBrokers)
	if len(brokers) == 0 {
		return map[string]any{"status": "unavailable", "error": "no brokers configured"}
	}

	conn, err := segmentio.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return map[string]any{"status": "unavailable", "error": err.Error()}
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(c.cfg.KafkaLogTopic)
	if err != nil {
		return map[string]any{"status": "unavailable", "topic": c.cfg.KafkaLogTopic, "error": err.Error()}
	}

	var depth int64
	partitionCount := 0
	for _, partition := range partitions {
		if partition.Topic != c.cfg.KafkaLogTopic {
			continue
		}
		partitionCount++
		leader, err := segmentio.DialLeader(ctx, "tcp", brokers[0], partition.Topic, partition.ID)
		if err != nil {
			continue
		}
		first, last, err := leader.ReadOffsets()
		_ = leader.Close()
		if err == nil && last > first {
			depth += last - first
		}
	}

	return map[string]any{
		"status":          "ok",
		"topic":           c.cfg.KafkaLogTopic,
		"partitions":      partitionCount,
		"topic_depth":     depth,
		"consumer_group":  c.cfg.KafkaConsumer,
		"consumer_lag":    nil,
		"lag_note":        "consumer lag is available after the WP7 worker consumer group is implemented",
		"retry_backlog":   nil,
		"spooled_records": nil,
	}
}

func overviewStatus(services []ServiceStatus) string {
	for _, service := range services {
		if service.Status == "unavailable" {
			return "degraded"
		}
	}
	return "ok"
}

func processCPUSeconds() float64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	user := float64(usage.Utime.Sec) + float64(usage.Utime.Usec)/1_000_000
	system := float64(usage.Stime.Sec) + float64(usage.Stime.Usec)/1_000_000
	return user + system
}

func splitBrokers(value string) []string {
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}
	return brokers
}

func parseTimeRange(values map[string][]string) (time.Time, time.Time, error) {
	now := time.Now().UTC()
	from := now.Add(-15 * time.Minute)
	to := now

	if raw := first(values["from"]); raw != "" {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid from value")
		}
		from = parsed
	}
	if raw := first(values["to"]); raw != "" {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid to value")
		}
		to = parsed
	}
	if !from.Before(to) {
		return time.Time{}, time.Time{}, fmt.Errorf("from must be before to")
	}
	return from.UTC(), to.UTC(), nil
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}
