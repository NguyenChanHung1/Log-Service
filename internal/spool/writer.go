package spool

import (
	"bufio"
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

type State struct {
	Dir       string `json:"dir"`
	Mode      string `json:"mode"`
	UsedBytes int64  `json:"used_bytes"`
	MaxBytes  int64  `json:"max_bytes"`
	FileCount int    `json:"file_count"`
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
	tmpPath := filepath.Join(w.dir, name+".tmp")
	finalPath := filepath.Join(w.dir, name)
	if err := os.WriteFile(tmpPath, payload, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, finalPath)
}

func (w *Writer) State() State {
	used, count, err := dirUsage(w.dir)
	if err != nil {
		return State{Dir: w.dir, Mode: "unknown", MaxBytes: w.maxBytes}
	}
	return State{
		Dir:       w.dir,
		Mode:      mode(used, w.maxBytes),
		UsedBytes: used,
		MaxBytes:  w.maxBytes,
		FileCount: count,
	}
}

func ReadFile(path string) ([]logevent.Event, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var events []logevent.Event
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event logevent.Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, scanner.Err()
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
	size, _, err := dirUsage(dir)
	return size, err
}

func dirUsage(dir string) (int64, int, error) {
	var total int64
	var count int
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) == ".tmp" {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		count++
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	return total, count, err
}

func mode(used int64, maxBytes int64) string {
	if maxBytes <= 0 {
		if used > 0 {
			return "degraded"
		}
		return "normal"
	}
	ratio := float64(used) / float64(maxBytes)
	switch {
	case ratio >= 1:
		return "exhausted"
	case ratio >= 0.8:
		return "critical"
	case ratio > 0:
		return "degraded"
	default:
		return "normal"
	}
}
