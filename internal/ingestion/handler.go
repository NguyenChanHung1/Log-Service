package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"log-service/internal/logevent"
	"log-service/internal/logline"
)

var ErrUnavailable = errors.New("no durable write path available")

type Publisher interface {
	Publish(ctx context.Context, events []logevent.Event) error
}

type Spooler interface {
	Write(ctx context.Context, events []logevent.Event) error
}

type Handler struct {
	publisher      Publisher
	spooler        Spooler
	logger         *slog.Logger
	maxBodyBytes   int64
	maxBatchSize   int
	requestTimeout time.Duration
}

type HandlerOptions struct {
	Publisher      Publisher
	Spooler        Spooler
	Logger         *slog.Logger
	MaxBodyBytes   int64
	MaxBatchSize   int
	RequestTimeout time.Duration
}

func NewHandler(opts HandlerOptions) *Handler {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Handler{
		publisher:      opts.Publisher,
		spooler:        opts.Spooler,
		logger:         logger,
		maxBodyBytes:   opts.MaxBodyBytes,
		maxBatchSize:   opts.MaxBatchSize,
		requestTimeout: opts.RequestTimeout,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	maxBodyBytes := h.maxBodyBytes
	if maxBodyBytes <= 0 {
		maxBodyBytes = 1 << 20
	}

	var req Request
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		if strings.Contains(err.Error(), "http: request body too large") {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "malformed json")
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, "malformed json")
		return
	}

	events, err := h.validate(req)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	ctx := r.Context()
	if h.requestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.requestTimeout)
		defer cancel()
	}

	if h.publisher == nil {
		h.logger.Error("ingestion publisher is not configured")
		writeError(w, http.StatusServiceUnavailable, ErrUnavailable.Error())
		return
	}

	if err := h.publisher.Publish(ctx, events); err == nil {
		writeJSON(w, http.StatusAccepted, Response{
			Accepted: len(events),
			Storage:  "kafka",
		})
		return
	} else if h.spooler == nil {
		h.logger.Error("kafka publish failed and no spool is configured", "error", err)
		writeError(w, http.StatusServiceUnavailable, ErrUnavailable.Error())
		return
	} else {
		h.logger.Warn("kafka publish failed, falling back to durable spool", "error", err)
	}

	if err := h.spooler.Write(ctx, events); err != nil {
		h.logger.Error("durable spool write failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, ErrUnavailable.Error())
		return
	}

	writeJSON(w, http.StatusAccepted, Response{
		Accepted: len(events),
		Storage:  "spool",
		Degraded: true,
	})
}

func (h *Handler) validate(req Request) ([]logevent.Event, error) {
	source := strings.TrimSpace(req.Source)
	if source == "" {
		return nil, errors.New("source is required")
	}
	if len(req.Records) == 0 {
		return nil, errors.New("records must not be empty")
	}
	if h.maxBatchSize > 0 && len(req.Records) > h.maxBatchSize {
		return nil, fmt.Errorf("batch size exceeds limit: %d > %d", len(req.Records), h.maxBatchSize)
	}

	receivedAt := time.Now().UTC()
	events := make([]logevent.Event, 0, len(req.Records))
	for i, record := range req.Records {
		parsed, err := logline.Parse(record)
		if err != nil {
			return nil, fmt.Errorf("record %d is invalid: %w", i, err)
		}
		events = append(events, logevent.Event{
			Source:     source,
			Raw:        parsed.Raw,
			ReceivedAt: receivedAt,
		})
	}

	return events, nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, ErrorResponse{Error: message})
}
