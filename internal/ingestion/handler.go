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
	"log-service/internal/metrics"
	"log-service/internal/spool"
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
	metrics        *metrics.Counters
	inFlight       chan struct{}
	maxBodyBytes   int64
	maxBatchSize   int
	requestTimeout time.Duration
}

type HandlerOptions struct {
	Publisher      Publisher
	Spooler        Spooler
	Logger         *slog.Logger
	Metrics        *metrics.Counters
	MaxBodyBytes   int64
	MaxBatchSize   int
	RequestTimeout time.Duration
	MaxInFlight    int
}

func NewHandler(opts HandlerOptions) *Handler {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	handler := &Handler{
		publisher:      opts.Publisher,
		spooler:        opts.Spooler,
		logger:         logger,
		metrics:        opts.Metrics,
		maxBodyBytes:   opts.MaxBodyBytes,
		maxBatchSize:   opts.MaxBatchSize,
		requestTimeout: opts.RequestTimeout,
	}
	if opts.MaxInFlight > 0 {
		handler.inFlight = make(chan struct{}, opts.MaxInFlight)
	}
	return handler
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.observeRequest()
	h.observeSpoolState()

	if !h.acquire(w) {
		return
	}
	defer h.release()

	if r.Method != http.MethodPost {
		h.observeRejected(0)
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
		h.observeRejected(0)
		if strings.Contains(err.Error(), "http: request body too large") {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "malformed json")
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		h.observeRejected(len(req.Records))
		writeError(w, http.StatusBadRequest, "malformed json")
		return
	}

	events, err := h.validate(req)
	if err != nil {
		h.observeRejected(len(req.Records))
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
		h.observeRejected(len(events))
		h.logger.Error("ingestion publisher is not configured")
		writeError(w, http.StatusServiceUnavailable, ErrUnavailable.Error())
		return
	}

	if err := h.publisher.Publish(ctx, events); err == nil {
		h.observeAccepted(events, "kafka")
		writeJSON(w, http.StatusAccepted, Response{Accepted: len(events), Storage: "kafka"})
		return
	} else if h.spooler == nil {
		h.observeKafkaPublishError()
		h.observeRejected(len(events))
		h.logger.Error("kafka publish failed and no spool is configured", "error", err)
		writeError(w, http.StatusServiceUnavailable, ErrUnavailable.Error())
		return
	} else {
		h.observeKafkaPublishError()
		h.logger.Warn("kafka publish failed, falling back to durable spool", "error", err)
	}

	if err := h.spooler.Write(ctx, events); err != nil {
		h.observeSpoolWriteError()
		h.observeRejected(len(events))
		h.observeSpoolState()
		if errors.Is(err, spool.ErrFull) {
			w.Header().Set("Retry-After", "1")
			h.logger.Error("durable spool capacity exhausted", "error", err)
			writeError(w, http.StatusServiceUnavailable, "durable spool capacity exhausted")
			return
		}
		h.logger.Error("durable spool write failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, ErrUnavailable.Error())
		return
	}

	h.observeAccepted(events, "spool")
	h.observeSpoolState()
	writeJSON(w, http.StatusAccepted, Response{Accepted: len(events), Storage: "spool", Degraded: true})
}

func (h *Handler) acquire(w http.ResponseWriter) bool {
	if h.inFlight == nil {
		return true
	}
	select {
	case h.inFlight <- struct{}{}:
		return true
	default:
		h.observeRejected(0)
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "ingestion is saturated; retry after durable backlog drains")
		return false
	}
}

func (h *Handler) release() {
	if h.inFlight != nil {
		<-h.inFlight
	}
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
			Timestamp:  parsed.Timestamp,
			IP:         parsed.IP.String(),
			Method:     parsed.Method,
			Path:       parsed.Path,
			Status:     parsed.Status,
			ReceivedAt: receivedAt,
		})
	}

	return events, nil
}

func (h *Handler) observeRequest() {
	if h.metrics != nil {
		h.metrics.ObserveRequest()
	}
}

func (h *Handler) observeRejected(records int) {
	if h.metrics != nil {
		h.metrics.ObserveRejected(records)
	}
}

func (h *Handler) observeAccepted(events []logevent.Event, storage string) {
	if h.metrics != nil {
		h.metrics.ObserveAccepted(events, storage)
	}
}

func (h *Handler) observeKafkaPublishError() {
	if h.metrics != nil {
		h.metrics.ObserveKafkaPublishError()
	}
}

func (h *Handler) observeSpoolWriteError() {
	if h.metrics != nil {
		h.metrics.ObserveSpoolWriteError()
	}
}

func (h *Handler) observeSpoolState() {
	if h.metrics == nil || h.spooler == nil {
		return
	}
	stateProvider, ok := h.spooler.(interface{ State() spool.State })
	if !ok {
		return
	}
	state := stateProvider.State()
	h.metrics.ObserveSpoolState(metrics.SpoolState{
		Dir:       state.Dir,
		Mode:      state.Mode,
		UsedBytes: state.UsedBytes,
		MaxBytes:  state.MaxBytes,
		FileCount: state.FileCount,
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, ErrorResponse{Error: message})
}
