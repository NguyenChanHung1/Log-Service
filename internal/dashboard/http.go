package dashboard

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

type Server struct {
	collector *Collector
	logger    *slog.Logger
}

func NewServer(collector *Collector, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{collector: collector, logger: logger}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/api/overview", s.overview)
	mux.HandleFunc("/api/metrics", s.metrics)
	mux.HandleFunc("/api/logs", s.logs)
	mux.HandleFunc("/api/logs/stream", s.streamLogs)
	return cors(mux)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "dashboard-api"})
}

func (s *Server) overview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	writeJSON(w, http.StatusOK, s.collector.Overview(r.Context()))
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	from, to, err := parseTimeRange(r.URL.Query())
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	result, err := s.collector.Metrics(r.Context(), from, to, r.URL.Query().Get("ip"), r.URL.Query().Get("path"))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) logs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	from, to, err := parseTimeRange(r.URL.Query())
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	limit := parseLimit(r.URL.Query().Get("limit"), 100)
	result, err := s.collector.Logs(
		r.Context(),
		from,
		to,
		r.URL.Query().Get("ip"),
		r.URL.Query().Get("path"),
		r.URL.Query().Get("status"),
		limit,
	)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) streamLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming is not supported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	seen := make(map[string]struct{})
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			now := time.Now().UTC()
			result, err := s.collector.Logs(r.Context(), now.Add(-10*time.Minute), now, "", "", "", 25)
			if err != nil {
				s.writeEvent(w, "error", map[string]string{"error": err.Error()})
				flusher.Flush()
				continue
			}
			for i := len(result.Logs) - 1; i >= 0; i-- {
				record := result.Logs[i]
				key := record.Timestamp.Format(time.RFC3339Nano) + record.Raw
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				s.writeEvent(w, "log", record)
			}
			flusher.Flush()
		}
	}
}

func (s *Server) writeEvent(w http.ResponseWriter, event string, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		s.logger.Error("marshal sse payload failed", "error", err)
		return
	}
	fmt.Fprintf(w, "event: %s\n", event)
	fmt.Fprintf(w, "data: %s\n\n", data)
}

func parseLimit(raw string, fallback int) int {
	if raw == "" {
		return fallback
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 {
		return fallback
	}
	if limit > 500 {
		return 500
	}
	return limit
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func methodNotAllowed(w http.ResponseWriter, allowed string) {
	w.Header().Set("Allow", allowed)
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
