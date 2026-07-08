package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	segmentio "github.com/segmentio/kafka-go"

	"log-service/internal/logevent"
	"log-service/internal/logline"
	"log-service/internal/storage"
)

type Config struct {
	Brokers        string
	LogTopic       string
	DLQTopic       string
	ConsumerGroup  string
	BulkSize       int
	FlushInterval  time.Duration
	RetryMax       int
	ReplayDir      string
	ReplayInterval time.Duration
	WorkerID       string
}

type Service struct {
	cfg    Config
	reader *segmentio.Reader
	dlq    *segmentio.Writer
	es     *storage.Elasticsearch
	stats  *Stats
	logger *slog.Logger
}

type queuedMessage struct {
	message segmentio.Message
	event   logevent.Event
}

type DLQMessage struct {
	Reason      string          `json:"reason"`
	Topic       string          `json:"topic,omitempty"`
	Partition   int             `json:"partition,omitempty"`
	Offset      int64           `json:"offset,omitempty"`
	RawPayload  json.RawMessage `json:"raw_payload,omitempty"`
	Event       *logevent.Event `json:"event,omitempty"`
	FailedAt    time.Time       `json:"failed_at"`
	WorkerID    string          `json:"worker_id"`
	HTTPStatus  int             `json:"http_status,omitempty"`
	Recoverable bool            `json:"recoverable"`
}

func NewService(cfg Config, es *storage.Elasticsearch, stats *Stats, logger *slog.Logger) *Service {
	if cfg.BulkSize <= 0 {
		cfg.BulkSize = 1000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 2 * time.Second
	}
	if cfg.RetryMax < 0 {
		cfg.RetryMax = 0
	}
	if cfg.ReplayInterval <= 0 {
		cfg.ReplayInterval = 2 * time.Second
	}
	if cfg.WorkerID == "" {
		hostname, _ := os.Hostname()
		cfg.WorkerID = hostname
	}
	if logger == nil {
		logger = slog.Default()
	}

	brokers := splitBrokers(cfg.Brokers)
	return &Service{
		cfg: cfg,
		reader: segmentio.NewReader(segmentio.ReaderConfig{
			Brokers:        brokers,
			Topic:          cfg.LogTopic,
			GroupID:        cfg.ConsumerGroup,
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: 0,
		}),
		dlq: &segmentio.Writer{
			Addr:         segmentio.TCP(brokers...),
			Topic:        cfg.DLQTopic,
			Balancer:     &segmentio.Hash{},
			RequiredAcks: segmentio.RequireAll,
			Async:        false,
		},
		es:     es,
		stats:  stats,
		logger: logger,
	}
}

func (s *Service) Run(ctx context.Context) error {
	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		s.replayLoop(ctx)
	}()

	err := s.consumeLoop(ctx)
	<-replayDone
	return err
}

func (s *Service) Close() error {
	readerErr := s.reader.Close()
	dlqErr := s.dlq.Close()
	if readerErr != nil {
		return readerErr
	}
	return dlqErr
}

func (s *Service) Ready(ctx context.Context) error {
	if err := s.es.Ping(ctx); err != nil {
		return err
	}
	brokers := splitBrokers(s.cfg.Brokers)
	if len(brokers) == 0 {
		return errors.New("no kafka brokers configured")
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	return conn.Close()
}

func (s *Service) consumeLoop(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]queuedMessage, 0, s.cfg.BulkSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		s.stats.SetCurrentBatch(len(batch))
		if err := s.processBatch(ctx, batch); err != nil {
			s.logger.Error("worker batch failed without safe offset commit", "error", err)
			time.Sleep(time.Second)
			return
		}
		batch = batch[:0]
		s.stats.SetCurrentBatch(0)
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return nil
		case <-ticker.C:
			flush()
		default:
		}

		message, err := s.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				flush()
				return nil
			}
			s.logger.Warn("fetch kafka message failed", "error", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		s.stats.ObserveConsumed(1)

		event, err := s.decode(message)
		if err != nil {
			if dlqErr := s.sendDLQ(ctx, DLQMessage{
				Reason:      err.Error(),
				Topic:       message.Topic,
				Partition:   message.Partition,
				Offset:      message.Offset,
				RawPayload:  append(json.RawMessage(nil), message.Value...),
				FailedAt:    time.Now().UTC(),
				WorkerID:    s.cfg.WorkerID,
				Recoverable: false,
			}); dlqErr != nil {
				s.logger.Error("send parse failure to dlq failed", "error", dlqErr)
				continue
			}
			if err := s.reader.CommitMessages(ctx, message); err != nil {
				s.logger.Error("commit dlq message failed", "error", err)
				continue
			}
			s.stats.ObserveDLQ(1)
			s.stats.ObserveFailed(1)
			continue
		}

		batch = append(batch, queuedMessage{message: message, event: event})
		if len(batch) >= s.cfg.BulkSize {
			flush()
		}
	}
}

func (s *Service) decode(message segmentio.Message) (logevent.Event, error) {
	var event logevent.Event
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return logevent.Event{}, fmt.Errorf("invalid event json: %w", err)
	}
	parsed, err := logline.Parse(event.Raw)
	if err != nil {
		return logevent.Event{}, fmt.Errorf("invalid raw log line: %w", err)
	}
	event.Timestamp = parsed.Timestamp
	event.IP = parsed.IP.String()
	event.Method = parsed.Method
	event.Path = parsed.Path
	event.Status = parsed.Status
	event.ProcessedAt = time.Now().UTC()
	event.WorkerID = s.cfg.WorkerID
	return event, nil
}

func (s *Service) processBatch(ctx context.Context, batch []queuedMessage) error {
	events := make([]logevent.Event, 0, len(batch))
	messages := make([]segmentio.Message, 0, len(batch))
	for _, item := range batch {
		events = append(events, item.event)
		messages = append(messages, item.message)
	}

	if err := s.indexSafely(ctx, events); err != nil {
		return err
	}
	return s.reader.CommitMessages(ctx, messages...)
}

func (s *Service) indexSafely(ctx context.Context, events []logevent.Event) error {
	pending := events
	for attempt := 0; attempt <= s.cfg.RetryMax; attempt++ {
		result, err := s.es.BulkIndex(ctx, pending)
		if err != nil {
			s.stats.ObserveElasticsearchFailure()
			s.logger.Warn("elasticsearch bulk index failed", "attempt", attempt+1, "error", err)
		}
		if result.Indexed > 0 {
			s.stats.ObserveIndexed(result.Indexed)
		}
		if len(result.Permanent) > 0 {
			if err := s.sendPermanentFailures(ctx, result.Permanent); err != nil {
				return err
			}
		}
		if len(result.Retryable) == 0 && err == nil {
			return nil
		}

		if len(result.Retryable) > 0 {
			pending = result.Retryable
		}
		if len(pending) == 0 {
			return nil
		}
		if attempt == s.cfg.RetryMax {
			break
		}

		s.stats.ObserveRetried(len(pending))
		wait := backoff(attempt)
		if result.RetryAfter > 0 {
			wait = result.RetryAfter
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}

	if err := s.writeReplay(pending); err != nil {
		return fmt.Errorf("%w: replay spool write failed: %v", storage.ErrNoSafeStorage, err)
	}
	s.stats.ObserveFailed(len(pending))
	s.updateReplayBacklog()
	return nil
}

func (s *Service) sendPermanentFailures(ctx context.Context, failures []storage.DocumentFailure) error {
	for _, failure := range failures {
		event := failure.Event
		if err := s.sendDLQ(ctx, DLQMessage{
			Reason:      failure.Reason,
			Event:       &event,
			FailedAt:    time.Now().UTC(),
			WorkerID:    s.cfg.WorkerID,
			HTTPStatus:  failure.Status,
			Recoverable: false,
		}); err != nil {
			return err
		}
	}
	s.stats.ObserveDLQ(len(failures))
	s.stats.ObserveFailed(len(failures))
	return nil
}

func (s *Service) sendDLQ(ctx context.Context, payload DLQMessage) error {
	value, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	key := payload.WorkerID
	if payload.Event != nil {
		key = payload.Event.Source
	}
	return s.dlq.WriteMessages(ctx, segmentio.Message{
		Key:   []byte(key),
		Value: value,
		Time:  payload.FailedAt,
	})
}

func (s *Service) writeReplay(events []logevent.Event) error {
	if len(events) == 0 {
		return nil
	}
	if err := os.MkdirAll(s.cfg.ReplayDir, 0o755); err != nil {
		return err
	}
	name := fmt.Sprintf("%s-%d.jsonl", time.Now().UTC().Format("20060102T150405.000000000Z"), os.Getpid())
	path := filepath.Join(s.cfg.ReplayDir, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	for _, event := range events {
		if err := encoder.Encode(event); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) replayLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.ReplayInterval)
	defer ticker.Stop()
	s.updateReplayBacklog()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.replayOnce(ctx); err != nil {
				s.logger.Warn("worker replay pass failed", "error", err)
			}
			s.updateReplayBacklog()
		}
	}
}

func (s *Service) replayOnce(ctx context.Context) error {
	files, err := filepath.Glob(filepath.Join(s.cfg.ReplayDir, "*.jsonl"))
	if err != nil {
		return err
	}
	for _, path := range files {
		events, err := readReplayFile(path)
		if err != nil {
			s.logger.Error("read replay file failed", "path", path, "error", err)
			continue
		}
		if len(events) == 0 {
			_ = os.Remove(path)
			continue
		}
		result, err := s.es.BulkIndex(ctx, events)
		if err != nil || len(result.Retryable) > 0 {
			s.stats.ObserveElasticsearchFailure()
			continue
		}
		if len(result.Permanent) > 0 {
			if err := s.sendPermanentFailures(ctx, result.Permanent); err != nil {
				return err
			}
		}
		s.stats.ObserveIndexed(result.Indexed)
		s.stats.ObserveReplayed(len(events))
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	return nil
}

func readReplayFile(path string) ([]logevent.Event, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var events []logevent.Event
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var event logevent.Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, scanner.Err()
}

func (s *Service) updateReplayBacklog() {
	files, err := filepath.Glob(filepath.Join(s.cfg.ReplayDir, "*.jsonl"))
	if err != nil {
		return
	}
	s.stats.SetReplayBacklog(len(files))
}

func backoff(attempt int) time.Duration {
	wait := time.Duration(1<<attempt) * 200 * time.Millisecond
	if wait > 5*time.Second {
		return 5 * time.Second
	}
	return wait
}

func splitBrokers(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
