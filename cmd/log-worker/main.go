package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"log-service/internal/config"
	"log-service/internal/storage"
	"log-service/internal/worker"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	stats := worker.NewStats()
	es := storage.NewElasticsearch(cfg.ElasticsearchURL)
	service := worker.NewService(worker.Config{
		Brokers:        cfg.KafkaBrokers,
		LogTopic:       cfg.KafkaLogTopic,
		DLQTopic:       cfg.KafkaDLQTopic,
		ConsumerGroup:  cfg.KafkaConsumerGroup,
		BulkSize:       cfg.WorkerBulkSize,
		FlushInterval:  cfg.WorkerFlushInterval,
		RetryMax:       cfg.WorkerRetryMax,
		ReplayDir:      filepath.Join(cfg.SpoolDir, "worker-replay"),
		ReplayInterval: cfg.SpoolReplayInterval,
		WorkerID:       os.Getenv("HOSTNAME"),
	}, es, stats, logger)
	defer func() {
		if err := service.Close(); err != nil {
			logger.Warn("worker close failed", "error", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "log-worker"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		readyCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := service.Ready(readyCtx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unready", "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "log-worker"})
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, stats.Snapshot())
	})

	server := &http.Server{Addr: ":" + cfg.WorkerPort, Handler: mux}
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- service.Run(ctx)
	}()

	serverDone := make(chan error, 1)
	go func() {
		logger.Info("starting log worker", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	workerExited := false
	select {
	case <-ctx.Done():
	case err := <-workerDone:
		workerExited = true
		if err != nil {
			logger.Error("worker stopped", "error", err)
		}
		stop()
	case err := <-serverDone:
		if err != nil {
			logger.Error("worker http server stopped", "error", err)
		}
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("worker http shutdown failed", "error", err)
	}
	if !workerExited {
		<-workerDone
	}
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
