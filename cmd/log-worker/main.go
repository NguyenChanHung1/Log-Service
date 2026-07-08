package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"

	"log-service/internal/config"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "log-worker"})
	})

	addr := ":" + cfg.WorkerPort
	logger.Info("starting log worker", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("log worker stopped", "error", err)
		os.Exit(1)
	}
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
