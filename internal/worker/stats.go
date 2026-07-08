package worker

import (
	"runtime"
	"sync/atomic"
	"syscall"
	"time"
)

type Stats struct {
	started time.Time

	consumedRecords atomic.Int64
	indexedRecords  atomic.Int64
	failedRecords   atomic.Int64
	retriedRecords  atomic.Int64
	dlqRecords      atomic.Int64
	replayedRecords atomic.Int64
	replayBacklog   atomic.Int64
	currentBatch    atomic.Int64
	esFailures      atomic.Int64
}

type StatsSnapshot struct {
	UptimeSeconds       int64   `json:"uptime_seconds"`
	ConsumedRecords     int64   `json:"consumed_records"`
	IndexedRecords      int64   `json:"indexed_records"`
	FailedRecords       int64   `json:"failed_records"`
	RetriedRecords      int64   `json:"retried_records"`
	DLQRecords          int64   `json:"dlq_records"`
	ReplayedRecords     int64   `json:"replayed_records"`
	ReplayBacklog       int64   `json:"replay_backlog"`
	CurrentBatchSize    int64   `json:"current_batch_size"`
	ElasticsearchErrors int64   `json:"elasticsearch_errors"`
	Goroutines          int     `json:"goroutines"`
	MemoryAllocBytes    uint64  `json:"memory_alloc_bytes"`
	ProcessCPUSeconds   float64 `json:"process_cpu_seconds"`
	CPUPercentEstimate  float64 `json:"cpu_percent_estimate"`
}

func NewStats() *Stats {
	return &Stats{started: time.Now().UTC()}
}

func (s *Stats) ObserveConsumed(count int) {
	if count > 0 {
		s.consumedRecords.Add(int64(count))
	}
}

func (s *Stats) ObserveIndexed(count int) {
	if count > 0 {
		s.indexedRecords.Add(int64(count))
	}
}

func (s *Stats) ObserveFailed(count int) {
	if count > 0 {
		s.failedRecords.Add(int64(count))
	}
}

func (s *Stats) ObserveRetried(count int) {
	if count > 0 {
		s.retriedRecords.Add(int64(count))
	}
}

func (s *Stats) ObserveDLQ(count int) {
	if count > 0 {
		s.dlqRecords.Add(int64(count))
	}
}

func (s *Stats) ObserveReplayed(count int) {
	if count > 0 {
		s.replayedRecords.Add(int64(count))
	}
}

func (s *Stats) ObserveElasticsearchFailure() {
	s.esFailures.Add(1)
}

func (s *Stats) SetReplayBacklog(count int) {
	s.replayBacklog.Store(int64(count))
}

func (s *Stats) SetCurrentBatch(count int) {
	s.currentBatch.Store(int64(count))
}

func (s *Stats) Snapshot() StatsSnapshot {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	uptime := int64(time.Since(s.started).Seconds())
	cpuSeconds := processCPUSeconds()
	cpuPercent := 0.0
	if uptime > 0 {
		cpuPercent = cpuSeconds / float64(uptime) * 100
	}

	return StatsSnapshot{
		UptimeSeconds:       uptime,
		ConsumedRecords:     s.consumedRecords.Load(),
		IndexedRecords:      s.indexedRecords.Load(),
		FailedRecords:       s.failedRecords.Load(),
		RetriedRecords:      s.retriedRecords.Load(),
		DLQRecords:          s.dlqRecords.Load(),
		ReplayedRecords:     s.replayedRecords.Load(),
		ReplayBacklog:       s.replayBacklog.Load(),
		CurrentBatchSize:    s.currentBatch.Load(),
		ElasticsearchErrors: s.esFailures.Load(),
		Goroutines:          runtime.NumGoroutine(),
		MemoryAllocBytes:    mem.Alloc,
		ProcessCPUSeconds:   cpuSeconds,
		CPUPercentEstimate:  cpuPercent,
	}
}

func processCPUSeconds() float64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}

	user := float64(usage.Utime.Sec) + float64(usage.Utime.Usec)/1_000_000
	system := float64(usage.Stime.Sec) + float64(usage.Stime.Usec)/1_000_000
	return user + system
}
