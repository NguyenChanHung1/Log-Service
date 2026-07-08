package main

import (
	"flag"
	"fmt"
	"time"
)

func main() {
	target := flag.String("target", "http://localhost:8080/v1/logs", "log ingestion endpoint")
	clients := flag.Int("clients", 1, "number of concurrent clients")
	tps := flag.Int("tps", 100, "target transactions per second")
	duration := flag.Duration("duration", time.Minute, "generator run duration")
	batchSize := flag.Int("batch-size", 100, "records per request")
	mode := flag.String("mode", "steady", "traffic mode: steady or burst")
	flag.Parse()

	fmt.Printf("log generator skeleton\n")
	fmt.Printf("target=%s clients=%d tps=%d duration=%s batch_size=%d mode=%s\n",
		*target, *clients, *tps, *duration, *batchSize, *mode)
}
