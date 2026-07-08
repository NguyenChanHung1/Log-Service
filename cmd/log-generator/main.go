package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type requestPayload struct {
	Source  string   `json:"source"`
	Records []string `json:"records"`
}

type responsePayload struct {
	Accepted int    `json:"accepted"`
	Storage  string `json:"storage"`
}

type stats struct {
	generated      atomic.Int64
	sent           atomic.Int64
	accepted       atomic.Int64
	failedRequests atomic.Int64
	retries        atomic.Int64
	mu             sync.Mutex
	latencies      []time.Duration
}

func (s *stats) observeLatency(value time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latencies = append(s.latencies, value)
}

func (s *stats) summary(duration time.Duration) (avg time.Duration, p95 time.Duration, effectiveTPS float64) {
	s.mu.Lock()
	latencies := append([]time.Duration(nil), s.latencies...)
	s.mu.Unlock()
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		var total time.Duration
		for _, latency := range latencies {
			total += latency
		}
		avg = total / time.Duration(len(latencies))
		idx := int(float64(len(latencies))*0.95) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(latencies) {
			idx = len(latencies) - 1
		}
		p95 = latencies[idx]
	}
	if duration.Seconds() > 0 {
		effectiveTPS = float64(s.sent.Load()) / duration.Seconds()
	}
	return avg, p95, effectiveTPS
}

func main() {
	target := flag.String("target", "http://localhost:8080/v1/logs", "log ingestion endpoint")
	clients := flag.Int("clients", 1, "number of concurrent clients")
	tps := flag.Int("tps", 100, "target generated log records per second")
	duration := flag.Duration("duration", time.Minute, "generator run duration")
	batchSize := flag.Int("batch-size", 100, "records per request")
	mode := flag.String("mode", "steady", "traffic mode: steady or burst")
	flag.Parse()

	if *clients < 1 {
		*clients = 1
	}
	if *tps < 1 {
		*tps = 1
	}
	if *batchSize < 1 {
		*batchSize = 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	started := time.Now()
	var wg sync.WaitGroup
	runStats := &stats{}
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			runClient(ctx, clientID, *target, *clients, *tps, *batchSize, *mode, runStats)
		}(i + 1)
	}
	wg.Wait()
	elapsed := time.Since(started)
	avg, p95, effectiveTPS := runStats.summary(elapsed)

	fmt.Printf("log generator summary\n")
	fmt.Printf("target=%s clients=%d target_tps=%d duration=%s batch_size=%d mode=%s\n", *target, *clients, *tps, elapsed.Round(time.Millisecond), *batchSize, *mode)
	fmt.Printf("generated_records=%d\n", runStats.generated.Load())
	fmt.Printf("sent_records=%d\n", runStats.sent.Load())
	fmt.Printf("accepted_records=%d\n", runStats.accepted.Load())
	fmt.Printf("failed_requests=%d\n", runStats.failedRequests.Load())
	fmt.Printf("retries=%d\n", runStats.retries.Load())
	fmt.Printf("average_latency=%s\n", avg.Round(time.Millisecond))
	fmt.Printf("p95_latency=%s\n", p95.Round(time.Millisecond))
	fmt.Printf("effective_tps=%.2f\n", effectiveTPS)
}

func runClient(ctx context.Context, clientID int, target string, clients int, totalTPS int, batchSize int, mode string, runStats *stats) {
	client := &http.Client{Timeout: 5 * time.Second}
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(clientID)))
	clientTPS := float64(totalTPS) / float64(clients)
	batchInterval := time.Duration(float64(time.Second) * float64(batchSize) / clientTPS)
	if batchInterval < 10*time.Millisecond {
		batchInterval = 10 * time.Millisecond
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		started := time.Now()
		if mode == "burst" {
			batches := int(math.Ceil(clientTPS / float64(batchSize)))
			if batches < 1 {
				batches = 1
			}
			for i := 0; i < batches; i++ {
				sendBatch(ctx, client, target, clientID, batchSize, rng, runStats)
			}
			if time.Since(started) < time.Second {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second - time.Since(started)):
				}
			}
			continue
		}

		sendBatch(ctx, client, target, clientID, batchSize, rng, runStats)

		remaining := batchInterval - time.Since(started)
		if remaining > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(remaining):
			}
		}
	}
}

func sendBatch(ctx context.Context, client *http.Client, target string, clientID int, batchSize int, rng *rand.Rand, runStats *stats) {
	records := make([]string, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		records = append(records, makeLogLine(rng))
	}
	runStats.generated.Add(int64(len(records)))

	payload, err := json.Marshal(requestPayload{Source: fmt.Sprintf("generator-%03d", clientID), Records: records})
	if err != nil {
		runStats.failedRequests.Add(1)
		return
	}

	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			runStats.retries.Add(1)
		}
		started := time.Now()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(payload))
		if err != nil {
			runStats.failedRequests.Add(1)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		res, err := client.Do(req)
		latency := time.Since(started)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			time.Sleep(backoff(attempt))
			continue
		}

		var response responsePayload
		_ = json.NewDecoder(res.Body).Decode(&response)
		_ = res.Body.Close()
		runStats.observeLatency(latency)
		if res.StatusCode >= 200 && res.StatusCode < 300 {
			runStats.sent.Add(int64(len(records)))
			runStats.accepted.Add(int64(response.Accepted))
			return
		}
		if res.StatusCode >= 500 || res.StatusCode == http.StatusTooManyRequests {
			time.Sleep(backoff(attempt))
			continue
		}
		runStats.failedRequests.Add(1)
		return
	}
	runStats.failedRequests.Add(1)
}

func makeLogLine(rng *rand.Rand) string {
	methods := []string{"GET", "POST", "PUT", "PATCH", "DELETE"}
	paths := []string{"/", "/login", "/logout", "/payment", "/checkout", "/search", "/profile", "/api/orders", "/api/items"}
	statuses := []int{200, 200, 200, 201, 204, 301, 400, 401, 403, 404, 429, 500, 502, 503}
	ip := fmt.Sprintf("10.%d.%d.%d", rng.Intn(32), rng.Intn(255), rng.Intn(255))
	return fmt.Sprintf("%s %s %s %s %d", time.Now().UTC().Format(time.RFC3339), ip, methods[rng.Intn(len(methods))], paths[rng.Intn(len(paths))], statuses[rng.Intn(len(statuses))])
}

func backoff(attempt int) time.Duration {
	return time.Duration(attempt+1) * 200 * time.Millisecond
}
