package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"log-service/internal/config"
	"log-service/internal/ingestion"
	"log-service/internal/kafka"
	"log-service/internal/metrics"
	"log-service/internal/spool"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	producer := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaLogTopic)
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Error("close kafka producer", "error", err)
		}
	}()

	apiMetrics := metrics.NewCounters("log-api")
	spooler := spool.NewWriter(cfg.SpoolDir, cfg.SpoolMaxBytes)
	apiMetrics.ObserveSpoolState(toMetricSpoolState(spooler.State()))
	logsHandler := ingestion.NewHandler(ingestion.HandlerOptions{
		Publisher:      producer,
		Spooler:        spooler,
		Logger:         logger,
		Metrics:        apiMetrics,
		MaxBodyBytes:   cfg.MaxBodyBytes,
		MaxBatchSize:   cfg.MaxBatchSize,
		RequestTimeout: cfg.RequestTimeout,
		MaxInFlight:    cfg.MaxInFlightRequests,
	})

	replayer := spool.NewReplayer(spool.ReplayerOptions{
		Dir:       cfg.SpoolDir,
		MaxBytes:  cfg.SpoolMaxBytes,
		Interval:  cfg.SpoolReplayInterval,
		Publisher: producer,
		Logger:    logger,
		OnReplay:  apiMetrics.ObserveReplayed,
		OnState: func(state spool.State) {
			apiMetrics.ObserveSpoolState(toMetricSpoolState(state))
		},
	})
	go replayer.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "log-api"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		readyCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := producer.Ping(readyCtx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unready", "dependency": "kafka", "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "log-api"})
	})
	mux.Handle("/stats", metrics.Handler(apiMetrics))
	mux.Handle("/v1/logs", logsHandler)

	addr := ":" + cfg.AppPort
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Info("starting log api", "addr", addr)
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("log api stopped", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
		os.Exit(1)
	}
	logger.Info("log api stopped")
}

func toMetricSpoolState(state spool.State) metrics.SpoolState {
	return metrics.SpoolState{
		Dir:       state.Dir,
		Mode:      state.Mode,
		UsedBytes: state.UsedBytes,
		MaxBytes:  state.MaxBytes,
		FileCount: state.FileCount,
	}
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
