package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"log-service/internal/logevent"
)

type Elasticsearch struct {
	baseURL string
	client  *http.Client
}

type BulkResult struct {
	Indexed    int
	Permanent  []DocumentFailure
	Retryable  []logevent.Event
	RetryAfter time.Duration
}

type DocumentFailure struct {
	Event  logevent.Event
	Status int
	Reason string
}

func NewElasticsearch(baseURL string) *Elasticsearch {
	return &Elasticsearch{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: 15 * time.Second},
	}
}

func (e *Elasticsearch) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.baseURL+"/_cluster/health", nil)
	if err != nil {
		return err
	}
	res, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 500 {
		return fmt.Errorf("elasticsearch returned %s", res.Status)
	}
	return nil
}

func (e *Elasticsearch) BulkIndex(ctx context.Context, events []logevent.Event) (BulkResult, error) {
	if len(events) == 0 {
		return BulkResult{}, nil
	}

	body, err := bulkBody(events)
	if err != nil {
		return BulkResult{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.baseURL+"/_bulk", bytes.NewReader(body))
	if err != nil {
		return BulkResult{}, err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	res, err := e.client.Do(req)
	if err != nil {
		return BulkResult{}, err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500 {
		return BulkResult{Retryable: events, RetryAfter: retryAfter(res.Header.Get("Retry-After"))}, fmt.Errorf("elasticsearch returned %s", res.Status)
	}
	if res.StatusCode >= 400 {
		return BulkResult{}, fmt.Errorf("elasticsearch returned %s", res.Status)
	}

	payload, err := io.ReadAll(res.Body)
	if err != nil {
		return BulkResult{}, err
	}

	var parsed bulkResponse
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return BulkResult{}, err
	}

	result := BulkResult{}
	for i, item := range parsed.Items {
		status, reason := item.statusReason()
		if status >= 200 && status < 300 {
			result.Indexed++
			continue
		}
		if i >= len(events) {
			continue
		}
		if status == http.StatusTooManyRequests || status >= 500 {
			result.Retryable = append(result.Retryable, events[i])
			continue
		}
		result.Permanent = append(result.Permanent, DocumentFailure{
			Event:  events[i],
			Status: status,
			Reason: reason,
		})
	}

	return result, nil
}

func bulkBody(events []logevent.Event) ([]byte, error) {
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	for _, event := range events {
		index := event.Timestamp.UTC().Format("logs-2006.01.02")
		if event.Timestamp.IsZero() {
			index = time.Now().UTC().Format("logs-2006.01.02")
		}
		if err := encoder.Encode(map[string]any{"index": map[string]string{"_index": index}}); err != nil {
			return nil, err
		}
		if err := encoder.Encode(event); err != nil {
			return nil, err
		}
	}
	return body.Bytes(), nil
}

func retryAfter(raw string) time.Duration {
	if raw == "" {
		return 0
	}
	duration, err := time.ParseDuration(raw + "s")
	if err == nil {
		return duration
	}
	return 0
}

type bulkResponse struct {
	Errors bool       `json:"errors"`
	Items  []bulkItem `json:"items"`
}

type bulkItem map[string]bulkItemResult

type bulkItemResult struct {
	Status int `json:"status"`
	Error  struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error"`
}

func (i bulkItem) statusReason() (int, string) {
	for _, result := range i {
		reason := result.Error.Reason
		if reason == "" {
			reason = result.Error.Type
		}
		return result.Status, reason
	}
	return 0, "missing bulk item result"
}

var ErrNoSafeStorage = errors.New("no safe storage path available")
