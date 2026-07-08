package spool

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"log-service/internal/logevent"
)

type Publisher interface {
	Publish(ctx context.Context, events []logevent.Event) error
}

type Replayer struct {
	dir       string
	interval  time.Duration
	publisher Publisher
	logger    *slog.Logger
	maxBytes  int64
	onReplay  func(records int)
	onState   func(State)
}

type ReplayerOptions struct {
	Dir       string
	Interval  time.Duration
	Publisher Publisher
	Logger    *slog.Logger
	MaxBytes  int64
	OnReplay  func(records int)
	OnState   func(State)
}

func NewReplayer(opts ReplayerOptions) *Replayer {
	if opts.Interval <= 0 {
		opts.Interval = 2 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Replayer{
		dir:       opts.Dir,
		interval:  opts.Interval,
		publisher: opts.Publisher,
		logger:    opts.Logger,
		maxBytes:  opts.MaxBytes,
		onReplay:  opts.OnReplay,
		onState:   opts.OnState,
	}
}

func (r *Replayer) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	r.publishState()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.ReplayOnce(ctx); err != nil {
				r.logger.Warn("spool replay pass failed", "error", err)
			}
			r.publishState()
		}
	}
}

func (r *Replayer) ReplayOnce(ctx context.Context) error {
	if r.publisher == nil {
		return nil
	}

	files, err := filepath.Glob(filepath.Join(r.dir, "*.jsonl"))
	if err != nil {
		return err
	}
	for _, path := range files {
		events, err := ReadFile(path)
		if err != nil {
			r.logger.Error("read spool file failed", "path", path, "error", err)
			continue
		}
		if len(events) == 0 {
			_ = os.Remove(path)
			continue
		}
		if err := r.publisher.Publish(ctx, events); err != nil {
			return err
		}
		if err := os.Remove(path); err != nil {
			return err
		}
		if r.onReplay != nil {
			r.onReplay(len(events))
		}
	}
	return nil
}

func (r *Replayer) publishState() {
	if r.onState == nil {
		return
	}
	writer := NewWriter(r.dir, r.maxBytes)
	r.onState(writer.State())
}
