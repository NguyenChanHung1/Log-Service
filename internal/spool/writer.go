package spool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"log-service/internal/logevent"
)

var ErrFull = errors.New("spool capacity exhausted")

type Writer struct {
	dir      string
	maxBytes int64
	now      func() time.Time
}

func NewWriter(dir string, maxBytes int64) *Writer {
	return &Writer{
		dir:      dir,
		maxBytes: maxBytes,
		now:      time.Now,
	}
}

func (w *Writer) Write(ctx context.Context, events []logevent.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}
	if err := os.MkdirAll(w.dir, 0o755); err != nil {
		return err
	}

	payload, err := marshalJSONLines(events)
	if err != nil {
		return err
	}
	if w.maxBytes > 0 {
		used, err := dirSize(w.dir)
		if err != nil {
			return err
		}
		if used+int64(len(payload)) > w.maxBytes {
			return ErrFull
		}
	}

	name := fmt.Sprintf("%s-%d.jsonl", w.now().UTC().Format("20060102T150405.000000000Z"), os.Getpid())
	path := filepath.Join(w.dir, name)
	return os.WriteFile(path, payload, 0o644)
}

func marshalJSONLines(events []logevent.Event) ([]byte, error) {
	var payload []byte
	for _, event := range events {
		line, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		payload = append(payload, line...)
		payload = append(payload, '\n')
	}
	return payload, nil
}

func dirSize(dir string) (int64, error) {
	var total int64
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}
