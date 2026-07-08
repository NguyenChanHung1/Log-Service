package spool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"log-service/internal/logevent"
)

func TestWriterPersistsEventsAsJSONLines(t *testing.T) {
	dir := t.TempDir()
	writer := NewWriter(dir, 1024)
	writer.now = func() time.Time {
		return time.Date(2026, 7, 7, 9, 0, 0, 0, time.UTC)
	}

	err := writer.Write(context.Background(), []logevent.Event{
		{
			Source:     "client-001",
			Raw:        "2026-07-07T09:00:01Z 10.10.1.5 GET /login 200",
			ReceivedAt: time.Date(2026, 7, 7, 9, 0, 1, 0, time.UTC),
		},
	})
	if err != nil {
		t.Fatalf("write spool: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read spool dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one spool file, got %d", len(entries))
	}

	content, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatalf("read spool file: %v", err)
	}
	if !strings.Contains(string(content), `"source":"client-001"`) {
		t.Fatalf("spool content missing source: %s", string(content))
	}
	if !strings.HasSuffix(string(content), "\n") {
		t.Fatalf("expected JSONL content to end with newline")
	}
}

func TestWriterReturnsFullWhenCapacityExceeded(t *testing.T) {
	dir := t.TempDir()
	writer := NewWriter(dir, 1)

	err := writer.Write(context.Background(), []logevent.Event{
		{
			Source:     "client-001",
			Raw:        "2026-07-07T09:00:01Z 10.10.1.5 GET /login 200",
			ReceivedAt: time.Date(2026, 7, 7, 9, 0, 1, 0, time.UTC),
		},
	})
	if !errors.Is(err, ErrFull) {
		t.Fatalf("expected ErrFull, got %v", err)
	}
}
