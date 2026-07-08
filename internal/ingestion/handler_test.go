package ingestion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"log-service/internal/logevent"
	"log-service/internal/metrics"
)

type fakePublisher struct {
	err    error
	events []logevent.Event
}

func (f *fakePublisher) Publish(ctx context.Context, events []logevent.Event) error {
	f.events = append([]logevent.Event(nil), events...)
	return f.err
}

type fakeSpooler struct {
	err    error
	events []logevent.Event
}

func (f *fakeSpooler) Write(ctx context.Context, events []logevent.Event) error {
	f.events = append([]logevent.Event(nil), events...)
	return f.err
}

func TestHandlerPublishesValidBatchToKafka(t *testing.T) {
	publisher := &fakePublisher{}
	handler := newTestHandler(publisher, &fakeSpooler{}, 1024, 10)

	response := postLogs(handler, `{
		"source": "client-001",
		"records": [
			"2026-07-07T09:00:01Z 10.10.1.5 GET /login 200",
			"2026-07-07T09:00:02Z 10.10.1.6 POST /payment 500"
		]
	}`)

	if response.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d: %s", response.Code, response.Body.String())
	}
	if len(publisher.events) != 2 {
		t.Fatalf("expected 2 published events, got %d", len(publisher.events))
	}
	if publisher.events[0].Source != "client-001" {
		t.Fatalf("expected source client-001, got %s", publisher.events[0].Source)
	}
	if publisher.events[0].Raw != "2026-07-07T09:00:01Z 10.10.1.5 GET /login 200" {
		t.Fatalf("unexpected raw event: %q", publisher.events[0].Raw)
	}

	var body Response
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Accepted != 2 || body.Storage != "kafka" || body.Degraded {
		t.Fatalf("unexpected response body: %+v", body)
	}
}

func TestHandlerRejectsMalformedJSON(t *testing.T) {
	handler := newTestHandler(&fakePublisher{}, &fakeSpooler{}, 1024, 10)

	response := postLogs(handler, `{`)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", response.Code)
	}
}

func TestHandlerRejectsInvalidRecord(t *testing.T) {
	publisher := &fakePublisher{}
	handler := newTestHandler(publisher, &fakeSpooler{}, 1024, 10)

	response := postLogs(handler, `{
		"source": "client-001",
		"records": ["2026-07-07T09:00:01Z 999.10.1.5 GET /login 200"]
	}`)

	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected status 422, got %d: %s", response.Code, response.Body.String())
	}
	if len(publisher.events) != 0 {
		t.Fatalf("expected no events to be published")
	}
}

func TestHandlerRejectsOversizedBody(t *testing.T) {
	handler := newTestHandler(&fakePublisher{}, &fakeSpooler{}, 16, 10)

	response := postLogs(handler, `{"source":"client-001","records":[]}`)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected status 413, got %d", response.Code)
	}
}

func TestHandlerRejectsOversizedBatch(t *testing.T) {
	handler := newTestHandler(&fakePublisher{}, &fakeSpooler{}, 1024, 1)

	response := postLogs(handler, `{
		"source": "client-001",
		"records": [
			"2026-07-07T09:00:01Z 10.10.1.5 GET /login 200",
			"2026-07-07T09:00:02Z 10.10.1.6 POST /payment 500"
		]
	}`)

	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected status 422, got %d", response.Code)
	}
}

func TestHandlerSpoolsWhenKafkaPublishFails(t *testing.T) {
	publisher := &fakePublisher{err: errors.New("kafka unavailable")}
	spooler := &fakeSpooler{}
	handler := newTestHandler(publisher, spooler, 1024, 10)

	response := postLogs(handler, `{
		"source": "client-001",
		"records": ["2026-07-07T09:00:01Z 10.10.1.5 GET /login 200"]
	}`)

	if response.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d: %s", response.Code, response.Body.String())
	}
	if len(spooler.events) != 1 {
		t.Fatalf("expected 1 spooled event, got %d", len(spooler.events))
	}

	var body Response
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Accepted != 1 || body.Storage != "spool" || !body.Degraded {
		t.Fatalf("unexpected response body: %+v", body)
	}
}

func TestHandlerReturnsUnavailableWhenKafkaAndSpoolFail(t *testing.T) {
	publisher := &fakePublisher{err: errors.New("kafka unavailable")}
	spooler := &fakeSpooler{err: errors.New("spool full")}
	handler := newTestHandler(publisher, spooler, 1024, 10)

	response := postLogs(handler, `{
		"source": "client-001",
		"records": ["2026-07-07T09:00:01Z 10.10.1.5 GET /login 200"]
	}`)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", response.Code)
	}
}

func TestHandlerRejectsNonPostMethods(t *testing.T) {
	handler := newTestHandler(&fakePublisher{}, &fakeSpooler{}, 1024, 10)
	request := httptest.NewRequest(http.MethodGet, "/v1/logs", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status 405, got %d", response.Code)
	}
}

func TestHandlerRecordsMetrics(t *testing.T) {
	publisher := &fakePublisher{err: errors.New("kafka unavailable")}
	spooler := &fakeSpooler{}
	counters := metrics.NewCounters("log-api")
	handler := NewHandler(HandlerOptions{
		Publisher:      publisher,
		Spooler:        spooler,
		Logger:         slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil)),
		Metrics:        counters,
		MaxBodyBytes:   1024,
		MaxBatchSize:   10,
		RequestTimeout: time.Second,
	})

	response := postLogs(handler, `{
		"source": "client-001",
		"records": ["2026-07-07T09:00:01Z 10.10.1.5 GET /login 200"]
	}`)
	if response.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d", response.Code)
	}

	snapshot := counters.Snapshot()
	if snapshot.RequestsTotal != 1 {
		t.Fatalf("expected 1 request, got %d", snapshot.RequestsTotal)
	}
	if snapshot.AcceptedRecordsTotal != 1 {
		t.Fatalf("expected 1 accepted record, got %d", snapshot.AcceptedRecordsTotal)
	}
	if snapshot.KafkaPublishErrorsTotal != 1 {
		t.Fatalf("expected 1 kafka publish error, got %d", snapshot.KafkaPublishErrorsTotal)
	}
	if snapshot.SpooledRecordsTotal != 1 {
		t.Fatalf("expected 1 spooled record, got %d", snapshot.SpooledRecordsTotal)
	}
	if snapshot.AverageBatchSize != 1 {
		t.Fatalf("expected average batch size 1, got %f", snapshot.AverageBatchSize)
	}
	if len(snapshot.RequestDimensionCounters) != 1 {
		t.Fatalf("expected one request dimension counter, got %d", len(snapshot.RequestDimensionCounters))
	}
	dimension := snapshot.RequestDimensionCounters[0]
	if dimension.Source != "client-001" || dimension.IP != "10.10.1.5" || dimension.Method != "GET" || dimension.Path != "/login" || dimension.Status != 200 {
		t.Fatalf("unexpected request dimension: %+v", dimension)
	}
}

func newTestHandler(publisher Publisher, spooler Spooler, maxBodyBytes int64, maxBatchSize int) *Handler {
	return NewHandler(HandlerOptions{
		Publisher:      publisher,
		Spooler:        spooler,
		Logger:         slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil)),
		MaxBodyBytes:   maxBodyBytes,
		MaxBatchSize:   maxBatchSize,
		RequestTimeout: time.Second,
	})
}

func postLogs(handler http.Handler, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/v1/logs", strings.NewReader(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
