package config

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("APP_PORT", "")
	t.Setenv("WORKER_PORT", "")
	t.Setenv("DASHBOARD_API_PORT", "")
	t.Setenv("DASHBOARD_UI_PORT", "")
	t.Setenv("MAX_IN_FLIGHT_REQUESTS", "")
	t.Setenv("LOG_API_URL", "")
	t.Setenv("WORKER_API_URL", "")
	t.Setenv("MAX_BATCH_SIZE", "")
	t.Setenv("REQUEST_TIMEOUT", "")
	t.Setenv("SPOOL_DIR", "")
	t.Setenv("SPOOL_MAX_BYTES", "")
	t.Setenv("SPOOL_REPLAY_INTERVAL", "")
	t.Setenv("REALTIME_STREAM_BUFFER", "")

	cfg := Load()

	if cfg.AppPort != "8080" {
		t.Fatalf("expected default app port 8080, got %s", cfg.AppPort)
	}
	if cfg.WorkerPort != "8081" {
		t.Fatalf("expected default worker port 8081, got %s", cfg.WorkerPort)
	}
	if cfg.DashboardAPIPort != "8082" {
		t.Fatalf("expected default dashboard api port 8082, got %s", cfg.DashboardAPIPort)
	}
	if cfg.DashboardUIPort != "3000" {
		t.Fatalf("expected default dashboard ui port 3000, got %s", cfg.DashboardUIPort)
	}
	if cfg.MaxInFlightRequests != 256 {
		t.Fatalf("expected default max in-flight requests 256, got %d", cfg.MaxInFlightRequests)
	}
	if cfg.LogAPIURL != "http://localhost:8080" {
		t.Fatalf("expected default log api url http://localhost:8080, got %s", cfg.LogAPIURL)
	}
	if cfg.WorkerAPIURL != "http://localhost:8081" {
		t.Fatalf("expected default worker api url http://localhost:8081, got %s", cfg.WorkerAPIURL)
	}
	if cfg.MaxBatchSize != 1000 {
		t.Fatalf("expected default max batch size 1000, got %d", cfg.MaxBatchSize)
	}
	if cfg.RequestTimeout != 5*time.Second {
		t.Fatalf("expected default request timeout 5s, got %s", cfg.RequestTimeout)
	}
	if cfg.SpoolDir != "/tmp/log-service-spool" {
		t.Fatalf("expected default spool dir /tmp/log-service-spool, got %s", cfg.SpoolDir)
	}
	if cfg.SpoolMaxBytes != 1073741824 {
		t.Fatalf("expected default spool max bytes 1073741824, got %d", cfg.SpoolMaxBytes)
	}
	if cfg.SpoolReplayInterval != 2*time.Second {
		t.Fatalf("expected default spool replay interval 2s, got %s", cfg.SpoolReplayInterval)
	}
	if cfg.RealtimeStreamBuffer != 1000 {
		t.Fatalf("expected default realtime stream buffer 1000, got %d", cfg.RealtimeStreamBuffer)
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("APP_PORT", "9000")
	t.Setenv("WORKER_PORT", "9001")
	t.Setenv("DASHBOARD_API_PORT", "9002")
	t.Setenv("DASHBOARD_UI_PORT", "9003")
	t.Setenv("MAX_IN_FLIGHT_REQUESTS", "7")
	t.Setenv("LOG_API_URL", "http://log-api:8080")
	t.Setenv("WORKER_API_URL", "http://log-worker:8081")
	t.Setenv("MAX_BATCH_SIZE", "42")
	t.Setenv("REQUEST_TIMEOUT", "250ms")
	t.Setenv("SPOOL_DIR", "/tmp/spool")
	t.Setenv("SPOOL_MAX_BYTES", "2048")
	t.Setenv("SPOOL_REPLAY_INTERVAL", "750ms")
	t.Setenv("REALTIME_STREAM_BUFFER", "25")

	cfg := Load()

	if cfg.AppPort != "9000" {
		t.Fatalf("expected app port override 9000, got %s", cfg.AppPort)
	}
	if cfg.WorkerPort != "9001" {
		t.Fatalf("expected worker port override 9001, got %s", cfg.WorkerPort)
	}
	if cfg.DashboardAPIPort != "9002" {
		t.Fatalf("expected dashboard api port override 9002, got %s", cfg.DashboardAPIPort)
	}
	if cfg.DashboardUIPort != "9003" {
		t.Fatalf("expected dashboard ui port override 9003, got %s", cfg.DashboardUIPort)
	}
	if cfg.MaxInFlightRequests != 7 {
		t.Fatalf("expected max in-flight requests override 7, got %d", cfg.MaxInFlightRequests)
	}
	if cfg.LogAPIURL != "http://log-api:8080" {
		t.Fatalf("expected log api url override http://log-api:8080, got %s", cfg.LogAPIURL)
	}
	if cfg.WorkerAPIURL != "http://log-worker:8081" {
		t.Fatalf("expected worker api url override http://log-worker:8081, got %s", cfg.WorkerAPIURL)
	}
	if cfg.MaxBatchSize != 42 {
		t.Fatalf("expected max batch size override 42, got %d", cfg.MaxBatchSize)
	}
	if cfg.RequestTimeout != 250*time.Millisecond {
		t.Fatalf("expected request timeout override 250ms, got %s", cfg.RequestTimeout)
	}
	if cfg.SpoolDir != "/tmp/spool" {
		t.Fatalf("expected spool dir override /tmp/spool, got %s", cfg.SpoolDir)
	}
	if cfg.SpoolMaxBytes != 2048 {
		t.Fatalf("expected spool max bytes override 2048, got %d", cfg.SpoolMaxBytes)
	}
	if cfg.SpoolReplayInterval != 750*time.Millisecond {
		t.Fatalf("expected spool replay interval override 750ms, got %s", cfg.SpoolReplayInterval)
	}
	if cfg.RealtimeStreamBuffer != 25 {
		t.Fatalf("expected realtime stream buffer override 25, got %d", cfg.RealtimeStreamBuffer)
	}
}

func TestLoadMatchesDotEnvFile(t *testing.T) {
	values, err := readDotEnv("../../.env")
	if err != nil {
		if os.IsNotExist(err) {
			t.Skip(".env file is not present")
		}
		t.Fatalf("read .env: %v", err)
	}

	for key, value := range values {
		t.Setenv(key, value)
	}

	cfg := Load()

	assertString(t, "APP_PORT", values, cfg.AppPort)
	assertString(t, "WORKER_PORT", values, cfg.WorkerPort)
	assertString(t, "DASHBOARD_API_PORT", values, cfg.DashboardAPIPort)
	assertString(t, "DASHBOARD_UI_PORT", values, cfg.DashboardUIPort)
	assertInt(t, "MAX_IN_FLIGHT_REQUESTS", values, cfg.MaxInFlightRequests)
	assertString(t, "LOG_API_URL", values, cfg.LogAPIURL)
	assertString(t, "WORKER_API_URL", values, cfg.WorkerAPIURL)
	assertString(t, "KAFKA_BROKERS", values, cfg.KafkaBrokers)
	assertString(t, "KAFKA_LOG_TOPIC", values, cfg.KafkaLogTopic)
	assertString(t, "KAFKA_RETRY_TOPIC", values, cfg.KafkaRetryTopic)
	assertString(t, "KAFKA_DLQ_TOPIC", values, cfg.KafkaDLQTopic)
	assertString(t, "KAFKA_CONSUMER_GROUP", values, cfg.KafkaConsumerGroup)
	assertString(t, "ELASTICSEARCH_URL", values, cfg.ElasticsearchURL)
	assertString(t, "SPOOL_DIR", values, cfg.SpoolDir)

	assertInt(t, "MAX_BATCH_SIZE", values, cfg.MaxBatchSize)
	assertInt64(t, "MAX_BODY_BYTES", values, cfg.MaxBodyBytes)
	assertDuration(t, "REQUEST_TIMEOUT", values, cfg.RequestTimeout)
	assertInt(t, "WORKER_BULK_SIZE", values, cfg.WorkerBulkSize)
	assertDuration(t, "WORKER_FLUSH_INTERVAL", values, cfg.WorkerFlushInterval)
	assertInt(t, "WORKER_RETRY_MAX", values, cfg.WorkerRetryMax)
	assertInt64(t, "SPOOL_MAX_BYTES", values, cfg.SpoolMaxBytes)
	assertDuration(t, "SPOOL_REPLAY_INTERVAL", values, cfg.SpoolReplayInterval)
	assertInt(t, "REALTIME_STREAM_BUFFER", values, cfg.RealtimeStreamBuffer)
}

func readDotEnv(path string) (map[string]string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	values := make(map[string]string)
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		values[strings.TrimSpace(key)] = strings.Trim(strings.TrimSpace(value), "\"'")
	}
	return values, nil
}

func assertString(t *testing.T, key string, values map[string]string, got string) {
	t.Helper()

	if got != values[key] {
		t.Fatalf("expected %s=%q, got %q", key, values[key], got)
	}
}

func assertInt(t *testing.T, key string, values map[string]string, got int) {
	t.Helper()

	want, err := strconv.Atoi(values[key])
	if err != nil {
		t.Fatalf("expected %s to be a valid int: %v", key, err)
	}
	if got != want {
		t.Fatalf("expected %s=%d, got %d", key, want, got)
	}
}

func assertInt64(t *testing.T, key string, values map[string]string, got int64) {
	t.Helper()

	want, err := strconv.ParseInt(values[key], 10, 64)
	if err != nil {
		t.Fatalf("expected %s to be a valid int64: %v", key, err)
	}
	if got != want {
		t.Fatalf("expected %s=%d, got %d", key, want, got)
	}
}

func assertDuration(t *testing.T, key string, values map[string]string, got time.Duration) {
	t.Helper()

	want, err := time.ParseDuration(values[key])
	if err != nil {
		t.Fatalf("expected %s to be a valid duration: %v", key, err)
	}
	if got != want {
		t.Fatalf("expected %s=%s, got %s", key, want, got)
	}
}
