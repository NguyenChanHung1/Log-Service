package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	AppPort              string
	WorkerPort           string
	DashboardAPIPort     string
	DashboardUIPort      string
	KafkaBrokers         string
	KafkaLogTopic        string
	KafkaRetryTopic      string
	KafkaDLQTopic        string
	KafkaConsumerGroup   string
	ElasticsearchURL     string
	MaxBatchSize         int
	MaxBodyBytes         int64
	RequestTimeout       time.Duration
	WorkerBulkSize       int
	WorkerFlushInterval  time.Duration
	WorkerRetryMax       int
	SpoolDir             string
	SpoolMaxBytes        int64
	SpoolReplayInterval  time.Duration
	RealtimeStreamBuffer int
}

func Load() Config {
	return Config{
		AppPort:              getString("APP_PORT", "8080"),
		WorkerPort:           getString("WORKER_PORT", "8081"),
		DashboardAPIPort:     getString("DASHBOARD_API_PORT", "8082"),
		DashboardUIPort:      getString("DASHBOARD_UI_PORT", "3000"),
		KafkaBrokers:         getString("KAFKA_BROKERS", "localhost:29092"),
		KafkaLogTopic:        getString("KAFKA_LOG_TOPIC", "logs.raw"),
		KafkaRetryTopic:      getString("KAFKA_RETRY_TOPIC", "logs.retry"),
		KafkaDLQTopic:        getString("KAFKA_DLQ_TOPIC", "logs.dlq"),
		KafkaConsumerGroup:   getString("KAFKA_CONSUMER_GROUP", "log-workers"),
		ElasticsearchURL:     getString("ELASTICSEARCH_URL", "http://localhost:9200"),
		MaxBatchSize:         getInt("MAX_BATCH_SIZE", 1000),
		MaxBodyBytes:         getInt64("MAX_BODY_BYTES", 1048576),
		RequestTimeout:       getDuration("REQUEST_TIMEOUT", 5*time.Second),
		WorkerBulkSize:       getInt("WORKER_BULK_SIZE", 1000),
		WorkerFlushInterval:  getDuration("WORKER_FLUSH_INTERVAL", 2*time.Second),
		WorkerRetryMax:       getInt("WORKER_RETRY_MAX", 5),
		SpoolDir:             getString("SPOOL_DIR", "/data/log-service-spool"),
		SpoolMaxBytes:        getInt64("SPOOL_MAX_BYTES", 1073741824),
		SpoolReplayInterval:  getDuration("SPOOL_REPLAY_INTERVAL", 2*time.Second),
		RealtimeStreamBuffer: getInt("REALTIME_STREAM_BUFFER", 1000),
	}
}

func getString(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getInt64(key string, fallback int64) int64 {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func getDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
