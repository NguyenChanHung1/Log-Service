package main

import (
	"log/slog"
	"net/http"
	"os"

	"log-service/internal/config"
	"log-service/internal/dashboard"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	collector := dashboard.NewCollector(dashboard.Config{
		ElasticsearchURL: cfg.ElasticsearchURL,
		KafkaBrokers:     cfg.KafkaBrokers,
		KafkaLogTopic:    cfg.KafkaLogTopic,
		KafkaConsumer:    cfg.KafkaConsumerGroup,
		LogAPIURL:        cfg.LogAPIURL,
		WorkerAPIURL:     cfg.WorkerAPIURL,
		StreamBuffer:     cfg.RealtimeStreamBuffer,
	})
	server := dashboard.NewServer(collector, logger)

	addr := ":" + cfg.DashboardAPIPort
	logger.Info("starting dashboard api", "addr", addr)
	if err := http.ListenAndServe(addr, server.Routes()); err != nil {
		logger.Error("dashboard api stopped", "error", err)
		os.Exit(1)
	}
}
