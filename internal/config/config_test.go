package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("APP_PORT", "")
	t.Setenv("WORKER_PORT", "")
	t.Setenv("MAX_BATCH_SIZE", "")
	t.Setenv("REQUEST_TIMEOUT", "")

	cfg := Load()

	if cfg.AppPort != "8080" {
		t.Fatalf("expected default app port 8080, got %s", cfg.AppPort)
	}
	if cfg.WorkerPort != "8081" {
		t.Fatalf("expected default worker port 8081, got %s", cfg.WorkerPort)
	}
	if cfg.MaxBatchSize != 1000 {
		t.Fatalf("expected default max batch size 1000, got %d", cfg.MaxBatchSize)
	}
	if cfg.RequestTimeout != 5*time.Second {
		t.Fatalf("expected default request timeout 5s, got %s", cfg.RequestTimeout)
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("APP_PORT", "9000")
	t.Setenv("WORKER_PORT", "9001")
	t.Setenv("MAX_BATCH_SIZE", "42")
	t.Setenv("REQUEST_TIMEOUT", "250ms")

	cfg := Load()

	if cfg.AppPort != "9000" {
		t.Fatalf("expected app port override 9000, got %s", cfg.AppPort)
	}
	if cfg.WorkerPort != "9001" {
		t.Fatalf("expected worker port override 9001, got %s", cfg.WorkerPort)
	}
	if cfg.MaxBatchSize != 42 {
		t.Fatalf("expected max batch size override 42, got %d", cfg.MaxBatchSize)
	}
	if cfg.RequestTimeout != 250*time.Millisecond {
		t.Fatalf("expected request timeout override 250ms, got %s", cfg.RequestTimeout)
	}
}
