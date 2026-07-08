package metrics

import (
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"log-service/internal/logevent"
)

type Counters struct {
	service string
	started time.Time

	requestsTotal           atomic.Int64
	acceptedRecordsTotal    atomic.Int64
	rejectedRequestsTotal   atomic.Int64
	rejectedRecordsTotal    atomic.Int64
	kafkaPublishErrorsTotal atomic.Int64
	spooledRecordsTotal     atomic.Int64
	spoolWriteErrorsTotal   atomic.Int64
	replayedRecordsTotal    atomic.Int64
	batchesAcceptedTotal    atomic.Int64
	batchRecordsTotal       atomic.Int64

	mu         sync.Mutex
	dimensions map[dimensionKey]int64
}

type Snapshot struct {
	Service                  string           `json:"service"`
	UptimeSeconds            int64            `json:"uptime_seconds"`
	RequestsTotal            int64            `json:"requests_total"`
	AcceptedRecordsTotal     int64            `json:"accepted_records_total"`
	RejectedRequestsTotal    int64            `json:"rejected_requests_total"`
	RejectedRecordsTotal     int64            `json:"rejected_records_total"`
	KafkaPublishErrorsTotal  int64            `json:"kafka_publish_errors_total"`
	SpooledRecordsTotal      int64            `json:"spooled_records_total"`
	SpoolWriteErrorsTotal    int64            `json:"spool_write_errors_total"`
	ReplayedRecordsTotal     int64            `json:"replayed_records_total"`
	BatchesAcceptedTotal     int64            `json:"batches_accepted_total"`
	AverageBatchSize         float64          `json:"average_batch_size"`
	Goroutines               int              `json:"goroutines"`
	MemoryAllocBytes         uint64           `json:"memory_alloc_bytes"`
	ProcessCPUSeconds        float64          `json:"process_cpu_seconds"`
	CPUPercentEstimate       float64          `json:"cpu_percent_estimate"`
	RequestDimensionCounters []DimensionCount `json:"request_dimension_counters"`
}

type DimensionCount struct {
	TimeBucket string `json:"time_bucket"`
	Source     string `json:"source"`
	IP         string `json:"ip"`
	Method     string `json:"method"`
	Path       string `json:"path"`
	Status     int    `json:"status"`
	Count      int64  `json:"count"`
}

type dimensionKey struct {
	timeBucket string
	source     string
	ip         string
	method     string
	path       string
	status     int
}

func NewCounters(service string) *Counters {
	return &Counters{
		service:    service,
		started:    time.Now().UTC(),
		dimensions: make(map[dimensionKey]int64),
	}
}

func (c *Counters) ObserveRequest() {
	c.requestsTotal.Add(1)
}

func (c *Counters) ObserveRejected(records int) {
	c.rejectedRequestsTotal.Add(1)
	if records > 0 {
		c.rejectedRecordsTotal.Add(int64(records))
	}
}

func (c *Counters) ObserveKafkaPublishError() {
	c.kafkaPublishErrorsTotal.Add(1)
}

func (c *Counters) ObserveSpoolWriteError() {
	c.spoolWriteErrorsTotal.Add(1)
}

func (c *Counters) ObserveAccepted(events []logevent.Event, storage string) {
	count := int64(len(events))
	c.acceptedRecordsTotal.Add(count)
	c.batchesAcceptedTotal.Add(1)
	c.batchRecordsTotal.Add(count)

	if storage == "spool" {
		c.spooledRecordsTotal.Add(count)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, event := range events {
		bucketTime := event.Timestamp
		if bucketTime.IsZero() {
			bucketTime = event.ReceivedAt
		}
		key := dimensionKey{
			timeBucket: bucketTime.UTC().Truncate(time.Minute).Format(time.RFC3339),
			source:     event.Source,
			ip:         event.IP,
			method:     event.Method,
			path:       event.Path,
			status:     event.Status,
		}
		c.dimensions[key]++
	}
}

func (c *Counters) ObserveReplayed(records int) {
	if records > 0 {
		c.replayedRecordsTotal.Add(int64(records))
	}
}

func (c *Counters) Snapshot() Snapshot {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	batches := c.batchesAcceptedTotal.Load()
	batchRecords := c.batchRecordsTotal.Load()
	avgBatchSize := 0.0
	if batches > 0 {
		avgBatchSize = float64(batchRecords) / float64(batches)
	}

	cpuSeconds := processCPUSeconds()
	uptimeSeconds := int64(time.Since(c.started).Seconds())
	cpuPercent := 0.0
	if uptimeSeconds > 0 {
		cpuPercent = (cpuSeconds / float64(uptimeSeconds)) * 100
	}

	return Snapshot{
		Service:                  c.service,
		UptimeSeconds:            uptimeSeconds,
		RequestsTotal:            c.requestsTotal.Load(),
		AcceptedRecordsTotal:     c.acceptedRecordsTotal.Load(),
		RejectedRequestsTotal:    c.rejectedRequestsTotal.Load(),
		RejectedRecordsTotal:     c.rejectedRecordsTotal.Load(),
		KafkaPublishErrorsTotal:  c.kafkaPublishErrorsTotal.Load(),
		SpooledRecordsTotal:      c.spooledRecordsTotal.Load(),
		SpoolWriteErrorsTotal:    c.spoolWriteErrorsTotal.Load(),
		ReplayedRecordsTotal:     c.replayedRecordsTotal.Load(),
		BatchesAcceptedTotal:     batches,
		AverageBatchSize:         avgBatchSize,
		Goroutines:               runtime.NumGoroutine(),
		MemoryAllocBytes:         mem.Alloc,
		ProcessCPUSeconds:        cpuSeconds,
		CPUPercentEstimate:       cpuPercent,
		RequestDimensionCounters: c.dimensionSnapshot(),
	}
}

func (c *Counters) dimensionSnapshot() []DimensionCount {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]DimensionCount, 0, len(c.dimensions))
	for key, count := range c.dimensions {
		out = append(out, DimensionCount{
			TimeBucket: key.timeBucket,
			Source:     key.source,
			IP:         key.ip,
			Method:     key.method,
			Path:       key.path,
			Status:     key.status,
			Count:      count,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].TimeBucket != out[j].TimeBucket {
			return out[i].TimeBucket < out[j].TimeBucket
		}
		if out[i].Source != out[j].Source {
			return out[i].Source < out[j].Source
		}
		if out[i].IP != out[j].IP {
			return out[i].IP < out[j].IP
		}
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		if out[i].Method != out[j].Method {
			return out[i].Method < out[j].Method
		}
		return out[i].Status < out[j].Status
	})

	return out
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
